# api/feedback.py
"""
Analyst Feedback & Explanation API
==================================
FastAPI web service providing:
1. GET  /api/alerts/{alert_id}/explanation: Fetch SHAP + GNN explanation report.
2. POST /api/alerts/{alert_id}/disposition: Analyst disposition submitter to Kafka 'analyst-feedback'.
"""

import json
import logging
import os
from typing import Optional, Dict, Any
from enum import Enum

from fastapi import FastAPI, HTTPException, Path, BackgroundTasks
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

# Global Explainer instance
explainer = HybridExplainer()

# Kafka Producer initialization (lazy / resilient)
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
            # Mock producer for local/testing environments without Kafka
            class MockProducer:
                def send(self, topic, value):
                    logger.info(f"[MOCK KAFKA] Sent to {topic}: {value}")
                    return None
                def flush(self):
                    pass
            producer = MockProducer()
    return producer


# Data Models
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


# Endpoints
@app.get("/")
def health_check():
    return {"status": "HEALTHY", "service": "Analyst Feedback & XAI API"}


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
