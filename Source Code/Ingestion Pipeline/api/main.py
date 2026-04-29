"""
FastAPI Control Plane
=====================
REST API for triggering, monitoring, and managing the data ingestion pipeline.
Provides endpoints to:
  - Trigger Airflow DAG runs
  - Publish custom events to Kafka
  - Query pipeline metrics and status
  - Health check all services
  - View dead letter queue
  - View pipeline alerts
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from pydantic import BaseModel, Field

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "pipeline_db")
DB_USER = os.getenv("DB_USER", "pipeline_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pipeline_pass")
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

# ─── FastAPI App ──────────────────────────────────────────────────────────────
app = FastAPI(
    title="📦 Data Ingestion Pipeline API",
    description="Control plane for the optimized ETL pipeline",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Shared Resources ─────────────────────────────────────────────────────────
_kafka_producer: Optional[KafkaProducer] = None


def get_kafka_producer() -> Optional[KafkaProducer]:
    global _kafka_producer
    if _kafka_producer is None:
        try:
            _kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
            )
        except NoBrokersAvailable:
            logger.warning("Kafka not available")
    return _kafka_producer


def get_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


# ─── Request/Response Models ──────────────────────────────────────────────────

class PublishEventRequest(BaseModel):
    event_type: str = Field(..., description="Type: 'order', 'user_activity', 'sensor'")
    payload: Dict[str, Any] = Field(..., description="Event payload")
    count: int = Field(1, ge=1, le=1000, description="Number of events to publish")


class TriggerDagRequest(BaseModel):
    dag_id: str = Field("optimized_ingestion_pipeline", description="Airflow DAG ID")
    conf: Optional[Dict[str, Any]] = Field(None, description="Optional DAG run config")


class MetricsResponse(BaseModel):
    total_raw_events: int
    total_processed_orders: int
    total_processed_activity: int
    total_processed_sensors: int
    total_dlq_events: int
    total_alerts: int
    watermarks: List[Dict]
    recent_metrics: List[Dict]


# ─── Root / Dashboard ─────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, tags=["Dashboard"])
async def dashboard():
    """HTML dashboard for quick status overview."""
    return """
    <!DOCTYPE html>
    <html>
    <head>
      <title>Data Ingestion Pipeline</title>
      <style>
        body { font-family: 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 2rem; }
        h1 { color: #38bdf8; font-size: 2rem; }
        .cards { display: flex; flex-wrap: wrap; gap: 1rem; margin: 1.5rem 0; }
        .card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 1.5rem; min-width: 200px; }
        .card h3 { margin: 0 0 0.5rem; color: #94a3b8; font-size: 0.85rem; text-transform: uppercase; }
        .card a { color: #38bdf8; text-decoration: none; font-size: 1.1rem; font-weight: 600; }
        .card a:hover { text-decoration: underline; }
        .badge { background: #22c55e; color: #000; padding: 2px 10px; border-radius: 999px; font-size: 0.75rem; font-weight: bold; }
        table { width: 100%; border-collapse: collapse; }
        th { text-align: left; color: #94a3b8; border-bottom: 1px solid #334155; padding: 0.5rem; }
        td { padding: 0.5rem; border-bottom: 1px solid #1e293b; }
      </style>
    </head>
    <body>
      <h1>🚀 Data Ingestion Pipeline <span class="badge">RUNNING</span></h1>
      <p>Production-grade ETL pipeline: Kafka → Consumer → PostgreSQL, orchestrated by Airflow.</p>
      <div class="cards">
        <div class="card"><h3>Airflow UI</h3><a href="http://localhost:8081" target="_blank">localhost:8081 →</a></div>
        <div class="card"><h3>Kafka UI</h3><a href="http://localhost:8080" target="_blank">localhost:8080 →</a></div>
        <div class="card"><h3>API Docs</h3><a href="/docs" target="_blank">/docs →</a></div>
        <div class="card"><h3>Metrics</h3><a href="/metrics" target="_blank">/metrics →</a></div>
        <div class="card"><h3>Health</h3><a href="/health" target="_blank">/health →</a></div>
      </div>
      <h2>Quick API Reference</h2>
      <table>
        <tr><th>Method</th><th>Endpoint</th><th>Description</th></tr>
        <tr><td>GET</td><td>/health</td><td>Health check all services</td></tr>
        <tr><td>GET</td><td>/metrics</td><td>Pipeline statistics</td></tr>
        <tr><td>POST</td><td>/pipeline/trigger</td><td>Trigger Airflow DAG</td></tr>
        <tr><td>POST</td><td>/events/publish</td><td>Publish events to Kafka</td></tr>
        <tr><td>GET</td><td>/pipeline/status</td><td>Latest DAG run status</td></tr>
        <tr><td>GET</td><td>/pipeline/dlq</td><td>Dead letter queue events</td></tr>
        <tr><td>GET</td><td>/pipeline/alerts</td><td>Pipeline alerts</td></tr>
      </table>
    </body>
    </html>
    """


# ─── Health Check ─────────────────────────────────────────────────────────────

@app.get("/health", tags=["Health"])
async def health_check():
    """Check health of all pipeline components."""
    status = {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat(), "services": {}}

    # PostgreSQL
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        status["services"]["postgresql"] = "✅ healthy"
    except Exception as e:
        status["services"]["postgresql"] = f"❌ {e}"
        status["status"] = "degraded"

    # Kafka
    try:
        p = get_kafka_producer()
        status["services"]["kafka"] = "✅ healthy" if p else "❌ unavailable"
        if not p:
            status["status"] = "degraded"
    except Exception as e:
        status["services"]["kafka"] = f"❌ {e}"
        status["status"] = "degraded"

    # Airflow
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(
                f"{AIRFLOW_BASE_URL}/api/v1/health",
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            )
            if r.status_code == 200:
                status["services"]["airflow"] = "✅ healthy"
            else:
                status["services"]["airflow"] = f"⚠️ status {r.status_code}"
    except Exception as e:
        status["services"]["airflow"] = f"❌ {e}"

    return status


# ─── Pipeline Metrics ─────────────────────────────────────────────────────────

@app.get("/metrics", tags=["Metrics"])
async def get_metrics():
    """Return comprehensive pipeline statistics."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM raw_events")
            total_raw = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM processed_orders")
            total_orders = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM processed_user_activity")
            total_activity = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM processed_sensor_readings")
            total_sensors = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM dead_letter_events")
            total_dlq = cur.fetchone()["cnt"]

            cur.execute("SELECT COUNT(*) AS cnt FROM alert_log WHERE resolved = FALSE")
            total_alerts = cur.fetchone()["cnt"]

            cur.execute("SELECT source_name, last_loaded_at, total_loaded FROM ingestion_watermarks ORDER BY source_name")
            watermarks = [dict(r) for r in cur.fetchall()]

            cur.execute("""
                SELECT pipeline_name, task_id, status, records_read, records_written,
                       records_failed, throughput_rps, processing_time_ms, started_at
                FROM pipeline_metrics
                ORDER BY started_at DESC
                LIMIT 20
            """)
            recent_metrics = []
            for r in cur.fetchall():
                row = dict(r)
                if row.get("started_at"):
                    row["started_at"] = row["started_at"].isoformat()
                recent_metrics.append(row)

            # Watermarks: convert datetimes to strings
            for w in watermarks:
                if w.get("last_loaded_at"):
                    w["last_loaded_at"] = w["last_loaded_at"].isoformat()

    finally:
        conn.close()

    return {
        "total_raw_events": total_raw,
        "total_processed_orders": total_orders,
        "total_processed_activity": total_activity,
        "total_processed_sensors": total_sensors,
        "total_dlq_events": total_dlq,
        "total_open_alerts": total_alerts,
        "watermarks": watermarks,
        "recent_metrics": recent_metrics,
    }


# ─── Pipeline Control ─────────────────────────────────────────────────────────

@app.post("/pipeline/trigger", tags=["Pipeline"])
async def trigger_dag(request: TriggerDagRequest):
    """
    Trigger an Airflow DAG run via the Airflow REST API.
    Useful for on-demand ingestion without waiting for schedule.
    """
    dag_run_id = f"api_trigger_{uuid.uuid4().hex[:8]}"
    payload = {
        "dag_run_id": dag_run_id,
        "conf": request.conf or {},
    }

    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.post(
                f"{AIRFLOW_BASE_URL}/api/v1/dags/{request.dag_id}/dagRuns",
                json=payload,
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            )
            if r.status_code in (200, 201):
                return {
                    "success": True,
                    "dag_run_id": dag_run_id,
                    "dag_id": request.dag_id,
                    "message": "DAG triggered successfully",
                    "airflow_response": r.json(),
                }
            else:
                raise HTTPException(
                    status_code=r.status_code,
                    detail=f"Airflow returned {r.status_code}: {r.text}",
                )
        except httpx.ConnectError:
            raise HTTPException(status_code=503, detail="Airflow is not reachable")


@app.get("/pipeline/status", tags=["Pipeline"])
async def get_dag_status(dag_id: str = "optimized_ingestion_pipeline"):
    """Get the last 5 DAG runs and their task statuses."""
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            r = await client.get(
                f"{AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}/dagRuns",
                params={"limit": 5, "order_by": "-execution_date"},
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
            )
            return r.json()
        except httpx.ConnectError:
            raise HTTPException(status_code=503, detail="Airflow is not reachable")


# ─── Event Publishing ─────────────────────────────────────────────────────────

TOPIC_MAP = {
    "order": "raw.orders",
    "user_activity": "raw.user-activity",
    "sensor": "raw.sensor-readings",
}


@app.post("/events/publish", tags=["Events"])
async def publish_event(request: PublishEventRequest):
    """
    Publish custom events directly to Kafka.
    Useful for testing and manual data injection.
    """
    topic = TOPIC_MAP.get(request.event_type)
    if not topic:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown event_type. Valid: {list(TOPIC_MAP.keys())}",
        )

    producer = get_kafka_producer()
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer not available")

    published = []
    for _ in range(request.count):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": request.event_type,
            "source": f"{request.event_type}_source",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": request.payload,
        }
        try:
            producer.send(topic, key=event["event_id"], value=event)
            published.append(event["event_id"])
        except KafkaError as e:
            raise HTTPException(status_code=500, detail=f"Kafka publish failed: {e}")

    producer.flush()
    return {
        "success": True,
        "topic": topic,
        "events_published": len(published),
        "event_ids": published[:10],  # Return first 10 for display
    }


# ─── Dead Letter Queue ────────────────────────────────────────────────────────

@app.get("/pipeline/dlq", tags=["Monitoring"])
async def get_dlq_events(limit: int = 50):
    """View events in the Dead Letter Queue (failed after max retries)."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, event_id, source, error_message, retry_count,
                       kafka_topic, failed_at
                FROM dead_letter_events
                ORDER BY failed_at DESC
                LIMIT %s
            """, (limit,))
            rows = []
            for r in cur.fetchall():
                row = dict(r)
                if row.get("failed_at"):
                    row["failed_at"] = row["failed_at"].isoformat()
                rows.append(row)
    finally:
        conn.close()

    return {"count": len(rows), "events": rows}


# ─── Alerts ───────────────────────────────────────────────────────────────────

@app.get("/pipeline/alerts", tags=["Monitoring"])
async def get_alerts(limit: int = 50, resolved: bool = False):
    """View pipeline alerts (SLA breaches, high failure rates, etc.)."""
    conn = get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, alert_type, severity, message, pipeline_name,
                       metric_value, threshold_value, resolved, created_at
                FROM alert_log
                WHERE resolved = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (resolved, limit))
            rows = []
            for r in cur.fetchall():
                row = dict(r)
                if row.get("created_at"):
                    row["created_at"] = row["created_at"].isoformat()
                rows.append(row)
    finally:
        conn.close()

    return {"count": len(rows), "alerts": rows}


# ─── Watermarks ───────────────────────────────────────────────────────────────

@app.delete("/pipeline/watermarks/reset", tags=["Pipeline"])
async def reset_watermarks():
    """
    Reset all ingestion watermarks to epoch (full reload).
    WARNING: This will cause the next pipeline run to re-ingest all data.
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE ingestion_watermarks
                SET last_loaded_at = '1970-01-01 00:00:00+00',
                    total_loaded = 0
            """)
        conn.commit()
    finally:
        conn.close()

    return {"success": True, "message": "All watermarks reset. Next run will do full reload."}


# ─── Startup Event ────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    logger.info("🚀 Pipeline API starting up...")
    get_kafka_producer()  # Pre-warm Kafka connection
    logger.info("✅ API ready")
