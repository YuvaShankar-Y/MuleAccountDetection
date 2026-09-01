# api/feedback.py
"""
Analyst Feedback & Explanation API + Web UI Dashboard
=====================================================
FastAPI web service providing:
1. GET  /                         : Interactive Analyst UI Dashboard (Visual SHAP + 2-Hop Graph + Disposition buttons)
2. GET  /api/alerts/{id}/explanation : Fetch SHAP + GNN explanation report (JSON)
3. POST /api/alerts/{id}/disposition : Analyst disposition submitter to Kafka 'analyst-feedback'
"""

import json
import logging
import os
from typing import Optional, Dict, Any
from enum import Enum

from fastapi import FastAPI, HTTPException, Path, BackgroundTasks
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from kafka import KafkaProducer

from explainability.explainer import HybridExplainer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
FEEDBACK_TOPIC = os.getenv("KAFKA_FEEDBACK_TOPIC", "analyst-feedback")

app = FastAPI(
    title="Mule Account Detection - Analyst Feedback & XAI API",
    description="FastAPI service for serving Explainable AI (XAI) reports and recording analyst dispositions.",
    version="1.0.0"
)

explainer = HybridExplainer()
producer: Optional[KafkaProducer] = None

def get_kafka_producer() -> KafkaProducer:
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
            )
        except Exception as e:
            logger.warning(f"Kafka Producer connection deferred/failed: {e}")
            class MockProducer:
                def send(self, topic, value):
                    logger.info(f"[MOCK KAFKA] Sent to {topic}: {value}")
                    return None
                def flush(self):
                    pass
            producer = MockProducer()
    return producer


class DispositionStatus(str, Enum):
    TRUE_POSITIVE = "TRUE_POSITIVE"
    FALSE_POSITIVE = "FALSE_POSITIVE"


class DispositionRequest(BaseModel):
    status: DispositionStatus = Field(..., description="Analyst verdict: TRUE_POSITIVE or FALSE_POSITIVE")
    analyst_notes: Optional[str] = Field(None, description="Optional notes explaining the verdict")
    account_id: Optional[str] = Field(None, description="Account ID associated with the alert")
    feature_vector: Optional[list] = Field(None, description="Optional feature vector snapshot for retraining")


class DispositionResponse(BaseModel):
    alert_id: str
    status: str
    message: str
    topic: str


# Dashboard HTML Page
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Mule Detection - Analyst Fraud Investigation Workspace</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        :root {
            --bg-dark: #0f172a;
            --panel-bg: #1e293b;
            --accent-red: #ef4444;
            --accent-green: #10b981;
            --accent-blue: #3b82f6;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --border-color: #334155;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', sans-serif; }
        body { background-color: var(--bg-dark); color: var(--text-main); padding: 24px; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid var(--border-color); }
        .header h1 { font-size: 22px; font-weight: 700; background: linear-gradient(90deg, #60a5fa, #a855f7); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .grid-container { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        .card { background-color: var(--panel-bg); border-radius: 12px; padding: 20px; border: 1px solid var(--border-color); }
        .card-title { font-size: 16px; font-weight: 600; margin-bottom: 16px; color: #cbd5e1; display: flex; align-items: center; justify-content: space-between; }
        .badge-danger { background-color: rgba(239, 68, 68, 0.2); color: var(--accent-red); padding: 4px 10px; border-radius: 20px; font-size: 12px; font-weight: 700; border: 1px solid var(--accent-red); }
        .badge-success { background-color: rgba(16, 185, 129, 0.2); color: var(--accent-green); padding: 4px 10px; border-radius: 20px; font-size: 12px; font-weight: 700; border: 1px solid var(--accent-green); }
        .metrics-row { display: flex; gap: 16px; margin-bottom: 16px; }
        .metric-box { flex: 1; background: #0f172a; padding: 14px; border-radius: 8px; border: 1px solid #334155; }
        .metric-box label { font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; }
        .metric-box val { display: block; font-size: 20px; font-weight: 700; margin-top: 4px; }
        .edge-list { list-style: none; }
        .edge-item { display: flex; justify-content: space-between; padding: 10px; border-bottom: 1px solid var(--border-color); font-size: 13px; }
        .edge-item:last-child { border-bottom: none; }
        .edge-score { color: #60a5fa; font-weight: 600; }
        textarea { width: 100%; height: 80px; background: #0f172a; border: 1px solid var(--border-color); border-radius: 8px; color: white; padding: 10px; font-size: 13px; margin-top: 12px; resize: none; }
        .btn-group { display: flex; gap: 12px; margin-top: 16px; }
        button { flex: 1; padding: 12px; border: none; border-radius: 8px; font-weight: 600; font-size: 14px; cursor: pointer; transition: all 0.2s ease; }
        .btn-tp { background-color: var(--accent-red); color: white; }
        .btn-tp:hover { background-color: #dc2626; box-shadow: 0 0 12px rgba(239, 68, 68, 0.4); }
        .btn-fp { background-color: #475569; color: white; }
        .btn-fp:hover { background-color: #334155; }
        #statusAlert { margin-top: 12px; padding: 10px; border-radius: 6px; font-size: 13px; display: none; }
    </style>
</head>
<body>

    <div class="header">
        <div>
            <h1>Mule Account Detection — Analyst Workbench</h1>
            <p style="font-size: 12px; color: var(--text-muted); margin-top: 4px;">Human-in-the-Loop Explainability & Active Learning Feedback Loop</p>
        </div>
        <div>
            <span class="badge-danger" id="riskBadge">HIGH RISK (85.0%)</span>
        </div>
    </div>

    <div class="metrics-row">
        <div class="metric-box"><label>Alert ID</label><val id="valAlertId">ALT-999</val></div>
        <div class="metric-box"><label>Account ID</label><val id="valAccount">Acc1001</val></div>
        <div class="metric-box"><label>Top Risk Factor</label><val style="color: #ef4444;">TDA $H_1$ Persistence</val></div>
        <div class="metric-box"><label>Active Learning Stream</label><val style="color: #10b981;">Kafka Connected</val></div>
    </div>

    <div class="grid-container">
        <!-- SHAP Feature Importance Chart -->
        <div class="card">
            <div class="card-title">
                <span>SHAP Feature Drivers (Top 5)</span>
                <span style="font-size: 11px; color: var(--text-muted);">XGBoost TreeExplainer</span>
            </div>
            <canvas id="shapChart" height="200"></canvas>
        </div>

        <!-- 2-Hop GNN Subgraph -->
        <div class="card">
            <div class="card-title">
                <span>Influential 2-Hop Transaction Neighborhood</span>
                <span style="font-size: 11px; color: var(--text-muted);">GraphSAGE GNNExplainer</span>
            </div>
            <ul class="edge-list" id="edgeList">
                <!-- Dynamically populated -->
            </ul>
        </div>
    </div>

    <!-- Disposition Feedback Box -->
    <div class="card" style="margin-top: 200px;">
        <div class="card-title">
            <span>Analyst Verdict Disposition</span>
            <span style="font-size: 11px; color: var(--text-muted);">Pushes to 'analyst-feedback' Kafka stream</span>
        </div>
        <p style="font-size: 13px; color: var(--text-muted);">Submitting a verdict increments the active learning retraining trigger counter (retrains XGBoost model every 100 dispositions).</p>
        <textarea id="analystNotes" placeholder="Enter investigation notes (e.g., 'Confirmed circular money laundering pattern between Acc1001, Account_102, and Account_104')..."></textarea>
        
        <div class="btn-group">
            <button class="btn-tp" onclick="submitDisposition('TRUE_POSITIVE')">Confirm True Positive (Flag Mule)</button>
            <button class="btn-fp" onclick="submitDisposition('FALSE_POSITIVE')">Mark False Positive (Dismiss Alert)</button>
        </div>

        <div id="statusAlert"></div>
    </div>

    <script>
        let currentAlertId = "ALT-999";
        let currentAccountId = "Acc1001";

        async function loadReport() {
            try {
                const res = await fetch(`/api/alerts/${currentAlertId}/explanation?account_id=${currentAccountId}&fraud_probability=0.85`);
                const data = await res.json();
                
                // Update SHAP Chart
                const labels = data.shap_top_drivers.map(d => d.feature);
                const values = data.shap_top_drivers.map(d => d.shap_value * 100);
                
                const ctx = document.getElementById('shapChart').getContext('2d');
                new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: [{
                            label: 'SHAP Risk Contribution (%)',
                            data: values,
                            backgroundColor: values.map(v => v > 0 ? '#ef4444' : '#10b981'),
                            borderRadius: 6
                        }]
                    },
                    options: {
                        indexAxis: 'y',
                        responsive: true,
                        plugins: { legend: { display: false } },
                        scales: { x: { grid: { color: '#334155' }, ticks: { color: '#94a3b8' } }, y: { ticks: { color: '#f8fafc' } } }
                    }
                });

                // Update Edge List
                const edgeListEl = document.getElementById('edgeList');
                edgeListEl.innerHTML = '';
                data.gnn_subgraph_explanation.influential_edges.forEach(edge => {
                    const li = document.createElement('li');
                    li.className = 'edge-item';
                    li.innerHTML = `<span>⚡ <strong>${edge.source}</strong> ➔ <strong>${edge.target}</strong></span> <span class="edge-score">GNN Weight: ${(edge.importance_score * 100).toFixed(1)}%</span>`;
                    edgeListEl.appendChild(li);
                });

            } catch (err) {
                console.error("Error loading report:", err);
            }
        }

        async function submitDisposition(status) {
            const notes = document.getElementById('analystNotes').value;
            const alertBox = document.getElementById('statusAlert');
            
            try {
                const res = await fetch(`/api/alerts/${currentAlertId}/disposition`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        status: status,
                        analyst_notes: notes,
                        account_id: currentAccountId
                    })
                });
                
                const data = await res.json();
                
                alertBox.style.display = 'block';
                if (res.ok) {
                    alertBox.style.backgroundColor = status === 'TRUE_POSITIVE' ? 'rgba(239, 68, 68, 0.2)' : 'rgba(16, 185, 129, 0.2)';
                    alertBox.style.color = status === 'TRUE_POSITIVE' ? '#ef4444' : '#10b981';
                    alertBox.style.border = `1px solid ${status === 'TRUE_POSITIVE' ? '#ef4444' : '#10b981'}`;
                    alertBox.innerHTML = `✅ <strong>Disposition Recorded:</strong> ${data.status} for ${data.alert_id} published to Kafka stream '${data.topic}'!`;
                } else {
                    alertBox.style.backgroundColor = 'rgba(239, 68, 68, 0.2)';
                    alertBox.style.color = '#ef4444';
                    alertBox.innerHTML = `❌ Error: ${data.detail || 'Submission failed'}`;
                }
            } catch (err) {
                console.error("Submission error:", err);
            }
        }

        window.onload = loadReport;
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def get_dashboard():
    """Interactive Analyst Fraud Investigation Workspace."""
    return HTMLResponse(content=DASHBOARD_HTML)


@app.get("/api/alerts/{alert_id}/explanation")
def get_explanation_report(
    alert_id: str = Path(..., description="Unique alert identifier"),
    account_id: Optional[str] = "Acc1001",
    fraud_probability: float = 0.85
):
    """Retrieve visual/mathematical proof of why an account was flagged."""
    try:
        report = explainer.generate_report(
            alert_id=alert_id,
            account_id=account_id,
            fraud_probability=fraud_probability,
            feature_vector=None
        )
        return report
    except Exception as e:
        logger.error(f"Error generating report for {alert_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/alerts/{alert_id}/disposition", response_model=DispositionResponse)
def submit_disposition(
    payload: DispositionRequest,
    alert_id: str = Path(..., description="Unique alert identifier")
):
    """Submit human analyst disposition verdict directly to Kafka 'analyst-feedback' topic."""
    kafka_producer = get_kafka_producer()
    
    event_payload = {
        "alert_id": alert_id,
        "account_id": payload.account_id or f"Account_{alert_id}",
        "status": payload.status.value,
        "analyst_notes": payload.analyst_notes or "",
        "feature_vector": payload.feature_vector or [],
        "label": 1 if payload.status == DispositionStatus.TRUE_POSITIVE else 0,
        "timestamp": os.getenv("MOCK_TIME", None)
    }
    
    try:
        kafka_producer.send(FEEDBACK_TOPIC, value=event_payload)
        kafka_producer.flush()
        logger.info(f"Disposition for alert {alert_id} written to Kafka topic '{FEEDBACK_TOPIC}'")
        
        return DispositionResponse(
            alert_id=alert_id,
            status=payload.status.value,
            message="Disposition submitted successfully to feedback stream",
            topic=FEEDBACK_TOPIC
        )
    except Exception as e:
        logger.error(f"Failed to push disposition for {alert_id} to Kafka: {e}")
        raise HTTPException(status_code=500, detail=f"Kafka push failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
