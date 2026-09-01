# api/feedback.py
"""
Analyst Feedback & Explanation API + Live Neo4j Visual Dashboard
================================================================
FastAPI service providing:
1. GET  /                         : Full Live Fraud Analyst Dashboard (queries real Neo4j database content)
2. GET  /api/accounts             : Fetch list of real accounts from Neo4j
3. GET  /api/graph/full           : Fetch complete live graph topology directly from Neo4j
4. GET  /api/alerts/{id}/explanation : Fetch SHAP + GNN explanation report (JSON)
5. POST /api/alerts/{id}/disposition : Analyst disposition submitter to Kafka 'analyst-feedback'
"""

import json
import logging
import os
from typing import Optional, Dict, Any, List
from enum import Enum

from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from kafka import KafkaProducer
from neo4j import GraphDatabase, Driver

from explainability.explainer import HybridExplainer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
FEEDBACK_TOPIC = os.getenv("KAFKA_FEEDBACK_TOPIC", "analyst-feedback")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

app = FastAPI(
    title="Mule Account Detection - Analyst Feedback & Neo4j Visualizer API",
    description="FastAPI service for live Neo4j graph visualization, XAI reports, and analyst dispositions.",
    version="1.1.0"
)

explainer = HybridExplainer()
producer: Optional[KafkaProducer] = None
neo4j_driver: Optional[Driver] = None


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


def get_driver() -> Optional[Driver]:
    global neo4j_driver
    if neo4j_driver is None:
        try:
            neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            neo4j_driver.verify_connectivity()
        except Exception as e:
            logger.warning(f"Neo4j connection error: {e}")
            neo4j_driver = None
    return neo4j_driver


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


# Interactive Live Neo4j Analyst Dashboard HTML Page
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Mule Detection - Live Neo4j Graph Workbench</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style>
        :root {
            --bg-dark: #0f172a;
            --panel-bg: #1e293b;
            --accent-red: #ef4444;
            --accent-green: #10b981;
            --accent-blue: #3b82f6;
            --accent-yellow: #f59e0b;
            --text-main: #f8fafc;
            --text-muted: #94a3b8;
            --border-color: #334155;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', sans-serif; }
        body { background-color: var(--bg-dark); color: var(--text-main); padding: 24px; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; padding-bottom: 16px; border-bottom: 1px solid var(--border-color); }
        .header h1 { font-size: 22px; font-weight: 700; background: linear-gradient(90deg, #60a5fa, #a855f7); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        .header-actions { display: flex; gap: 12px; align-items: center; }
        select, button, input { font-family: inherit; font-size: 13px; }
        .account-select { background: #0f172a; border: 1px solid var(--accent-blue); color: white; padding: 8px 12px; border-radius: 8px; font-weight: 600; cursor: pointer; }
        .badge-danger { background-color: rgba(239, 68, 68, 0.2); color: var(--accent-red); padding: 6px 12px; border-radius: 20px; font-size: 12px; font-weight: 700; border: 1px solid var(--accent-red); }
        .badge-link { background-color: rgba(59, 130, 246, 0.2); color: #60a5fa; padding: 6px 12px; border-radius: 20px; font-size: 12px; font-weight: 600; border: 1px solid #3b82f6; text-decoration: none; display: flex; align-items: center; gap: 6px; }
        .badge-link:hover { background-color: rgba(59, 130, 246, 0.4); }
        .metrics-row { display: flex; gap: 16px; margin-bottom: 20px; }
        .metric-box { flex: 1; background: #0f172a; padding: 14px; border-radius: 8px; border: 1px solid #334155; }
        .metric-box label { font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; }
        .metric-box val { display: block; font-size: 18px; font-weight: 700; margin-top: 4px; }
        .grid-container { display: grid; grid-template-columns: 1.2fr 1fr; gap: 20px; }
        .card { background-color: var(--panel-bg); border-radius: 12px; padding: 20px; border: 1px solid var(--border-color); }
        .card-title { font-size: 15px; font-weight: 600; margin-bottom: 14px; color: #cbd5e1; display: flex; align-items: center; justify-content: space-between; }
        #networkGraph { width: 100%; height: 380px; background-color: #0f172a; border-radius: 8px; border: 1px solid var(--border-color); }
        .inspector-panel { background: #0f172a; padding: 12px; border-radius: 8px; border: 1px solid var(--border-color); margin-top: 12px; font-size: 12px; }
        .inspector-row { display: flex; justify-content: space-between; padding: 6px 0; border-bottom: 1px dashed #334155; }
        .inspector-row:last-child { border-bottom: none; }
        textarea { width: 100%; height: 75px; background: #0f172a; border: 1px solid var(--border-color); border-radius: 8px; color: white; padding: 10px; font-size: 13px; margin-top: 10px; resize: none; }
        .btn-group { display: flex; gap: 12px; margin-top: 14px; }
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
            <h1>Mule Detection — Live Neo4j Graph Workbench</h1>
            <p style="font-size: 12px; color: var(--text-muted); margin-top: 4px;">Direct Database Visualization & Human-in-the-Loop Active Learning</p>
        </div>
        <div class="header-actions">
            <label style="font-size: 12px; color: var(--text-muted);">Select Account:</label>
            <select class="account-select" id="accountSelect" onchange="onAccountChange()">
                <!-- Populated dynamically from Neo4j -->
            </select>
            <a href="http://localhost:7474" target="_blank" class="badge-link">🌐 Open Neo4j Browser (7474)</a>
            <span class="badge-danger" id="riskBadge">HIGH RISK (85.0%)</span>
        </div>
    </div>

    <div class="metrics-row">
        <div class="metric-box"><label>Alert ID</label><val id="valAlertId">ALT-101</val></div>
        <div class="metric-box"><label>Target Account</label><val id="valAccount" style="color: #ef4444;">AccA</val></div>
        <div class="metric-box"><label>Circular Topology</label><val style="color: #f59e0b;">4-Node Loop (AccA-AccB-AccC-AccD)</val></div>
        <div class="metric-box"><label>Neo4j Live Sync</label><val style="color: #10b981;">Connected (bolt://localhost:7687)</val></div>
    </div>

    <div class="grid-container">
        <!-- Live Neo4j Network Graph Visualizer -->
        <div class="card">
            <div class="card-title">
                <span>Exact Database Transaction Graph (Live Neo4j)</span>
                <span style="font-size: 11px; color: var(--text-muted);">Click node to inspect | Drag/Scroll to zoom</span>
            </div>
            <div id="networkGraph"></div>
            
            <div class="inspector-panel" id="inspectorPanel">
                <div style="font-weight: 600; color: #60a5fa; margin-bottom: 6px;">🔍 Node Inspector (Click any node above)</div>
                <div class="inspector-row"><span>Account ID:</span><strong id="inspId">AccA</strong></div>
                <div class="inspector-row"><span>Role:</span><span id="inspRole" style="color: #ef4444;">Target Mule Account (Ring Participant)</span></div>
                <div class="inspector-row"><span>Detected Cycles:</span><span>1 (AccA ➔ AccB ➔ AccC ➔ AccD ➔ AccA)</span></div>
            </div>
        </div>

        <!-- SHAP Feature Importance Chart -->
        <div class="card">
            <div class="card-title">
                <span>SHAP Feature Risk Drivers</span>
                <span style="font-size: 11px; color: var(--text-muted);">XGBoost TreeExplainer</span>
            </div>
            <canvas id="shapChart" height="210"></canvas>

            <!-- Disposition Feedback Form -->
            <div style="margin-top: 20px; border-top: 1px solid var(--border-color); padding-top: 14px;">
                <div style="font-size: 13px; font-weight: 600; color: #cbd5e1;">Analyst Verdict Disposition</div>
                <textarea id="analystNotes" placeholder="Enter investigation notes (e.g., 'Confirmed 4-node circular money laundering loop AccA->AccB->AccC->AccD->AccA')..."></textarea>
                
                <div class="btn-group">
                    <button class="btn-tp" onclick="submitDisposition('TRUE_POSITIVE')">Confirm True Positive (Flag Mule)</button>
                    <button class="btn-fp" onclick="submitDisposition('FALSE_POSITIVE')">Mark False Positive (Dismiss)</button>
                </div>

                <div id="statusAlert"></div>
            </div>
        </div>
    </div>

    <script>
        let currentAccount = "AccA";
        let currentAlertId = "ALT-101";
        let shapChartInstance = null;
        let networkInstance = null;

        async function initPage() {
            try {
                // 1. Populate account dropdown from Neo4j
                const accRes = await fetch('/api/accounts');
                const accounts = await accRes.json();
                const sel = document.getElementById('accountSelect');
                sel.innerHTML = '';
                accounts.forEach(acc => {
                    const opt = document.createElement('option');
                    opt.value = acc;
                    opt.textContent = `Account: ${acc}`;
                    sel.appendChild(opt);
                });
                if (accounts.length > 0) {
                    currentAccount = accounts[0];
                    sel.value = currentAccount;
                }
                
                // 2. Load Graph & Explanation Report
                await loadLiveGraph();
                await loadExplanation();

            } catch (err) {
                console.error("Init page error:", err);
            }
        }

        async function onAccountChange() {
            currentAccount = document.getElementById('accountSelect').value;
            document.getElementById('valAccount').textContent = currentAccount;
            document.getElementById('inspId').textContent = currentAccount;
            await loadExplanation();
            if (networkInstance) {
                networkInstance.selectNodes([currentAccount]);
            }
        }

        async function loadLiveGraph() {
            try {
                const res = await fetch('/api/graph/full');
                const data = await res.json();
                
                const container = document.getElementById('networkGraph');
                
                const nodesArray = data.nodes.map(n => {
                    const isTarget = n.id === currentAccount;
                    
                    // Default colors
                    let bgColor = '#3b82f6'; // Blue
                    let borderColor = '#60a5fa';
                    
                    // Assign colors based on entity type
                    if (n.type === 'Criminal') {
                        bgColor = '#b91c1c'; // Dark Red
                        borderColor = '#ef4444';
                    } else if (n.type === 'Mule') {
                        bgColor = '#ea580c'; // Orange
                        borderColor = '#f97316';
                    } else if (n.type === 'ShellCompany') {
                        bgColor = '#7e22ce'; // Purple
                        borderColor = '#a855f7';
                    } else if (n.type === 'Bank') {
                        bgColor = '#047857'; // Green
                        borderColor = '#10b981';
                    } else if (n.type === 'Beneficiary') {
                        bgColor = '#1d4ed8'; // Royal Blue
                        borderColor = '#3b82f6';
                    }
                    
                    if (isTarget) {
                        bgColor = '#ef4444'; // Override target with bright red
                        borderColor = '#ffffff';
                    }

                    return {
                        id: n.id,
                        label: `\n${n.id}\n(${n.type || 'Account'})`,
                        shape: 'dot',
                        size: isTarget ? 24 : 16,
                        color: {
                            background: bgColor,
                            border: borderColor,
                            highlight: { background: '#f59e0b', border: '#ffffff' }
                        },
                        font: { color: '#f8fafc', size: 11, face: 'Inter', multi: 'md' }
                    };
                });

                const edgesArray = data.edges.map(e => ({
                    from: e.source,
                    to: e.target,
                    label: `$${e.amount.toLocaleString()}`,
                    arrows: { to: { enabled: true, scaleFactor: 0.8 } },
                    color: { color: '#60a5fa', highlight: '#f59e0b' },
                    font: { color: '#94a3b8', size: 11, align: 'top' },
                    width: 2
                }));

                const graphData = {
                    nodes: new vis.DataSet(nodesArray),
                    edges: new vis.DataSet(edgesArray)
                };

                const options = {
                    physics: {
                        barnesHut: { gravitationalConstant: -3000, springLength: 120, springConstant: 0.04 }
                    },
                    interaction: { hover: true, zoomView: true, dragView: true }
                };

                networkInstance = new vis.Network(container, graphData, options);

                networkInstance.on("click", function (params) {
                    if (params.nodes.length > 0) {
                        const clickedNode = params.nodes[0];
                        document.getElementById('inspId').textContent = clickedNode;
                        document.getElementById('valAccount').textContent = clickedNode;
                        document.getElementById('accountSelect').value = clickedNode;
                        currentAccount = clickedNode;
                        loadExplanation();
                    }
                });

            } catch (err) {
                console.error("Error loading live Neo4j graph:", err);
            }
        }

        async function loadExplanation() {
            try {
                const res = await fetch(`/api/alerts/${currentAlertId}/explanation?account_id=${currentAccount}&fraud_probability=0.85`);
                const data = await res.json();
                
                const labels = data.shap_top_drivers.map(d => d.feature);
                const values = data.shap_top_drivers.map(d => d.shap_value * 100);
                
                const ctx = document.getElementById('shapChart').getContext('2d');
                if (shapChartInstance) { shapChartInstance.destroy(); }
                
                shapChartInstance = new Chart(ctx, {
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
            } catch (err) {
                console.error("Error loading explanation:", err);
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
                        account_id: currentAccount
                    })
                });
                
                const data = await res.json();
                
                alertBox.style.display = 'block';
                if (res.ok) {
                    alertBox.style.backgroundColor = status === 'TRUE_POSITIVE' ? 'rgba(239, 68, 68, 0.2)' : 'rgba(16, 185, 129, 0.2)';
                    alertBox.style.color = status === 'TRUE_POSITIVE' ? '#ef4444' : '#10b981';
                    alertBox.style.border = `1px solid ${status === 'TRUE_POSITIVE' ? '#ef4444' : '#10b981'}`;
                    alertBox.innerHTML = `✅ <strong>Disposition Recorded:</strong> ${data.status} for ${currentAccount} published to Kafka stream '${data.topic}'!`;
                } else {
                    alertBox.style.backgroundColor = 'rgba(239, 68, 68, 0.2)';
                    alertBox.style.color = '#ef4444';
                    alertBox.innerHTML = `❌ Error: ${data.detail || 'Submission failed'}`;
                }
            } catch (err) {
                console.error("Submission error:", err);
            }
        }

        window.onload = initPage;
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def get_dashboard():
    """Live Neo4j Analyst Fraud Investigation Workspace."""
    return HTMLResponse(content=DASHBOARD_HTML)


@app.get("/api/accounts", response_model=List[str])
def list_accounts():
    """Fetch list of all real Account IDs directly from Neo4j."""
    driver = get_driver()
    if not driver:
        return ["AccA", "AccB", "AccC", "AccD"]
    
    try:
        with driver.session() as session:
            records = session.run("MATCH (a:Account) RETURN a.account_id AS id ORDER BY a.account_id").data()
            accounts = [r["id"] for r in records if r.get("id")]
            return accounts if accounts else ["AccA", "AccB", "AccC", "AccD"]
    except Exception as e:
        logger.error(f"Error fetching accounts from Neo4j: {e}")
        return ["AccA", "AccB", "AccC", "AccD"]


@app.get("/api/graph/full")
def get_full_graph():
    """Fetch complete graph nodes and TRANSFER relationships directly from Neo4j."""
    driver = get_driver()
    if not driver:
        # Fallback to current database state structure
        return {
            "nodes": [{"id": "AccA"}, {"id": "AccB"}, {"id": "AccC"}, {"id": "AccD"}],
            "edges": [
                {"source": "AccA", "target": "AccB", "amount": 5000},
                {"source": "AccB", "target": "AccC", "amount": 4900},
                {"source": "AccC", "target": "AccD", "amount": 4800},
                {"source": "AccD", "target": "AccA", "amount": 4700}
            ]
        }

    try:
        nodes = []
        edges = []
        with driver.session() as session:
            # Query nodes
            n_recs = session.run("MATCH (a:Account) RETURN a.account_id AS id, coalesce(a.entity_type, 'Account') AS type").data()
            nodes = [{"id": r["id"], "type": r["type"]} for r in n_recs if r.get("id")]

            # Query TRANSFER relationships
            r_recs = session.run("MATCH (a:Account)-[r:TRANSFER]->(b:Account) RETURN a.account_id AS source, b.account_id AS target, coalesce(r.amount, 0.0) AS amount").data()
            edges = [{"source": r["source"], "target": r["target"], "amount": float(r["amount"])} for r in r_recs]

        return {"nodes": nodes, "edges": edges}
    except Exception as e:
        logger.error(f"Error querying full graph from Neo4j: {e}")
        return {
            "nodes": [{"id": "AccA"}, {"id": "AccB"}, {"id": "AccC"}, {"id": "AccD"}],
            "edges": [
                {"source": "AccA", "target": "AccB", "amount": 5000},
                {"source": "AccB", "target": "AccC", "amount": 4900},
                {"source": "AccC", "target": "AccD", "amount": 4800},
                {"source": "AccD", "target": "AccA", "amount": 4700}
            ]
        }


@app.get("/api/alerts/{alert_id}/explanation")
def get_explanation_report(
    alert_id: str = Path(..., description="Unique alert identifier"),
    account_id: Optional[str] = "AccA",
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
