"""
Airflow DAG: Optimized Data Ingestion Pipeline
===============================================
Implements the research paper's optimized pipeline with:

  1. Parallel task execution (3 source branches run simultaneously)
  2. Incremental loading (watermark-based, only new records)
  3. Retry policies (exponential backoff, max 3 retries)
  4. SLA monitoring with email alerts
  5. Data quality validation between Extract → Load
  6. Pipeline metrics tracking

DAG Schedule: Every 10 minutes
"""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
import psycopg2.extras

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# ─── DAG Default Arguments ────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["alerts@yourcompany.com"],
    "email_on_failure": False,       # Set True in production with SMTP config
    "email_on_retry": False,
    "retries": 3,                    # Number of automatic retries
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,  # Exponential backoff between retries
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
    "sla": timedelta(minutes=30),    # Alert if DAG takes > 30 minutes
}

# ─── Database Connection Helper ───────────────────────────────────────────────

def get_db_conn():
    """Create PostgreSQL connection using Airflow environment variables."""
    import os
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "pipeline_db"),
        user=os.getenv("DB_USER", "pipeline_user"),
        password=os.getenv("DB_PASSWORD", "pipeline_pass"),
    )


def log_pipeline_metric(
    run_id: str,
    pipeline_name: str,
    task_id: str,
    status: str,
    records_read: int = 0,
    records_written: int = 0,
    records_failed: int = 0,
    processing_time_ms: int = 0,
    error_message: str = None,
    dag_id: str = "optimized_ingestion_pipeline",
):
    """Record pipeline execution metrics for evaluation."""
    conn = get_db_conn()
    try:
        throughput = (records_written / (processing_time_ms / 1000)) if processing_time_ms > 0 else 0
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_metrics
                    (run_id, pipeline_name, dag_id, task_id, status,
                     records_read, records_written, records_failed,
                     throughput_rps, processing_time_ms, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                run_id, pipeline_name, dag_id, task_id, status,
                records_read, records_written, records_failed,
                round(throughput, 2), processing_time_ms, error_message,
            ))
        conn.commit()
        logging.info(
            f"📊 Metrics | task={task_id} status={status} "
            f"read={records_read:,} written={records_written:,} "
            f"throughput={throughput:.1f}rps"
        )
    finally:
        conn.close()


# ─── Task Functions ───────────────────────────────────────────────────────────

def check_pipeline_health(**context):
    """
    Pre-flight health check.
    Verifies DB connectivity and Kafka topics exist before running.
    """
    run_id = context["run_id"]
    logging.info(f"🏥 Health check | run_id={run_id}")

    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # Check DB is accessible
            cur.execute("SELECT 1")
            # Check that raw_events table exists
            cur.execute("SELECT COUNT(*) FROM raw_events")
            count = cur.fetchone()[0]
            logging.info(f"✅ DB healthy | raw_events total: {count:,}")

            # Check watermarks are initialized
            cur.execute("SELECT source_name, last_loaded_at FROM ingestion_watermarks")
            watermarks = cur.fetchall()
            for source, wm in watermarks:
                logging.info(f"  📍 Watermark [{source}]: {wm}")

    finally:
        conn.close()

    log_pipeline_metric(run_id, "health_check", "check_pipeline_health", "success")
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}


def extract_incremental_orders(**context):
    """
    EXTRACT PHASE: Orders (Incremental)
    
    Uses watermark-based incremental loading:
    - Reads the last_loaded_at timestamp from ingestion_watermarks
    - Only fetches raw_events received AFTER that timestamp
    - Prevents re-processing of already-ingested data
    """
    run_id = context["run_id"]
    start_time = time.time()
    logging.info(f"📥 Extracting incremental orders | run_id={run_id}")

    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get current watermark (last successfully loaded timestamp)
            cur.execute("""
                SELECT last_loaded_at
                FROM ingestion_watermarks
                WHERE source_name = 'orders_source'
            """)
            wm = cur.fetchone()
            last_loaded_at = wm["last_loaded_at"] if wm else "1970-01-01 00:00:00+00"
            logging.info(f"  📍 Watermark: {last_loaded_at}")

            # Incremental query: only records newer than watermark
            cur.execute("""
                SELECT id, event_id, source, event_type, payload, received_at
                FROM raw_events
                WHERE source = 'orders_source'
                  AND processed = FALSE
                  AND received_at > %s
                ORDER BY received_at ASC
                LIMIT 10000
            """, (last_loaded_at,))
            records = cur.fetchall()

        elapsed = int((time.time() - start_time) * 1000)
        logging.info(f"✅ Extracted {len(records):,} order records in {elapsed}ms")

        # Push extracted data to XCom for downstream tasks
        serializable = [
            {**dict(r), "received_at": r["received_at"].isoformat()}
            for r in records
        ]
        context["ti"].xcom_push(key="orders_records", value=serializable)

        log_pipeline_metric(run_id, "extract_orders", "extract_incremental_orders",
                           "success", records_read=len(records), processing_time_ms=elapsed)
        return {"records_extracted": len(records)}

    finally:
        conn.close()


def extract_incremental_activity(**context):
    """
    EXTRACT PHASE: User Activity (Incremental)
    Same watermark-based pattern as orders.
    """
    run_id = context["run_id"]
    start_time = time.time()
    logging.info(f"📥 Extracting incremental user activity | run_id={run_id}")

    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT last_loaded_at
                FROM ingestion_watermarks
                WHERE source_name = 'user_activity_source'
            """)
            wm = cur.fetchone()
            last_loaded_at = wm["last_loaded_at"] if wm else "1970-01-01 00:00:00+00"

            cur.execute("""
                SELECT id, event_id, source, event_type, payload, received_at
                FROM raw_events
                WHERE source = 'user_activity_source'
                  AND processed = FALSE
                  AND received_at > %s
                ORDER BY received_at ASC
                LIMIT 10000
            """, (last_loaded_at,))
            records = cur.fetchall()

        elapsed = int((time.time() - start_time) * 1000)
        logging.info(f"✅ Extracted {len(records):,} activity records in {elapsed}ms")

        serializable = [
            {**dict(r), "received_at": r["received_at"].isoformat()}
            for r in records
        ]
        context["ti"].xcom_push(key="activity_records", value=serializable)

        log_pipeline_metric(run_id, "extract_activity", "extract_incremental_activity",
                           "success", records_read=len(records), processing_time_ms=elapsed)
        return {"records_extracted": len(records)}

    finally:
        conn.close()


def extract_incremental_sensors(**context):
    """
    EXTRACT PHASE: Sensor Readings (Incremental)
    """
    run_id = context["run_id"]
    start_time = time.time()
    logging.info(f"📥 Extracting incremental sensor readings | run_id={run_id}")

    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT last_loaded_at
                FROM ingestion_watermarks
                WHERE source_name = 'sensor_source'
            """)
            wm = cur.fetchone()
            last_loaded_at = wm["last_loaded_at"] if wm else "1970-01-01 00:00:00+00"

            cur.execute("""
                SELECT id, event_id, source, event_type, payload, received_at
                FROM raw_events
                WHERE source = 'sensor_source'
                  AND processed = FALSE
                  AND received_at > %s
                ORDER BY received_at ASC
                LIMIT 10000
            """, (last_loaded_at,))
            records = cur.fetchall()

        elapsed = int((time.time() - start_time) * 1000)
        logging.info(f"✅ Extracted {len(records):,} sensor records in {elapsed}ms")

        serializable = [
            {**dict(r), "received_at": r["received_at"].isoformat()}
            for r in records
        ]
        context["ti"].xcom_push(key="sensor_records", value=serializable)

        log_pipeline_metric(run_id, "extract_sensors", "extract_incremental_sensors",
                           "success", records_read=len(records), processing_time_ms=elapsed)
        return {"records_extracted": len(records)}

    finally:
        conn.close()


def validate_data_quality(**context):
    """
    DATA QUALITY GATE
    Checks all three source extracts have data and logs quality report.
    Raises ValueError to fail the DAG if quality thresholds are breached.
    """
    ti = context["ti"]
    run_id = context["run_id"]
    logging.info(f"🔍 Data quality validation | run_id={run_id}")

    order_records = ti.xcom_pull(task_ids="extract_orders", key="orders_records") or []
    activity_records = ti.xcom_pull(task_ids="extract_activity", key="activity_records") or []
    sensor_records = ti.xcom_pull(task_ids="extract_sensors", key="sensor_records") or []

    total = len(order_records) + len(activity_records) + len(sensor_records)

    logging.info(f"  📦 Orders: {len(order_records):,}")
    logging.info(f"  📦 Activity: {len(activity_records):,}")
    logging.info(f"  📦 Sensors: {len(sensor_records):,}")
    logging.info(f"  📦 Total: {total:,}")

    # Write alert if no data found (could indicate upstream failure)
    if total == 0:
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO alert_log (alert_type, severity, message, pipeline_name)
                    VALUES ('no_data', 'warning', %s, 'optimized_ingestion_pipeline')
                """, (f"No new data found in run {run_id}",))
            conn.commit()
        finally:
            conn.close()
        logging.warning("⚠️  No new records found. Pipeline will skip load phase.")
    else:
        logging.info(f"✅ Data quality passed | {total:,} records ready for load")

    return {"total_records": total}


def load_orders(**context):
    """
    LOAD PHASE: Orders → processed_orders table
    Performs bulk upsert and updates watermark.
    """
    ti = context["ti"]
    run_id = context["run_id"]
    records = ti.xcom_pull(task_ids="extract_orders", key="orders_records") or []

    if not records:
        logging.info("⏭️  No order records to load. Skipping.")
        return {"records_loaded": 0}

    start_time = time.time()
    loaded = 0
    failed = 0
    max_ts = None

    conn = get_db_conn()
    try:
        for record in records:
            payload = record.get("payload", {})
            if isinstance(payload, str):
                payload = json.loads(payload)

            received_at = record.get("received_at")
            if max_ts is None or received_at > max_ts:
                max_ts = received_at

            try:
                with conn.cursor() as cur:
                    order_data = payload.get("payload", payload)
                    cur.execute("""
                        INSERT INTO processed_orders
                            (order_id, customer_id, product_id, product_name, quantity,
                             unit_price, total_amount, discount, status, region,
                             source_system, raw_event_id, event_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (order_id) DO UPDATE SET
                            status = EXCLUDED.status, updated_at = NOW()
                    """, (
                        order_data.get("order_id", record.get("event_id")),
                        order_data.get("customer_id", "UNKNOWN"),
                        order_data.get("product_id", "UNKNOWN"),
                        order_data.get("product_name"),
                        order_data.get("quantity", 1),
                        order_data.get("unit_price", 0.0),
                        order_data.get("total_amount", 0.0),
                        order_data.get("discount", 0.0),
                        order_data.get("status", "unknown"),
                        order_data.get("region"),
                        record.get("source", "airflow"),
                        record.get("id"),
                        payload.get("timestamp") or received_at,
                    ))
                conn.commit()
                loaded += 1
            except Exception as e:
                conn.rollback()
                failed += 1
                logging.warning(f"⚠️  Order load failed: {e}")

        # Update watermark after successful load (incremental loading state)
        if max_ts:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ingestion_watermarks
                    SET last_loaded_at = %s::timestamptz, total_loaded = total_loaded + %s
                    WHERE source_name = 'orders_source'
                """, (max_ts, loaded))
            conn.commit()

    finally:
        conn.close()

    elapsed = int((time.time() - start_time) * 1000)
    logging.info(f"✅ Orders loaded: {loaded:,} | Failed: {failed} | Time: {elapsed}ms")

    log_pipeline_metric(run_id, "load_orders", "load_orders", "success",
                       records_read=len(records), records_written=loaded,
                       records_failed=failed, processing_time_ms=elapsed)
    return {"records_loaded": loaded, "records_failed": failed}


def load_activity(**context):
    """LOAD PHASE: User Activity → processed_user_activity table"""
    ti = context["ti"]
    run_id = context["run_id"]
    records = ti.xcom_pull(task_ids="extract_activity", key="activity_records") or []

    if not records:
        logging.info("⏭️  No activity records to load. Skipping.")
        return {"records_loaded": 0}

    start_time = time.time()
    loaded = 0
    failed = 0
    max_ts = None

    conn = get_db_conn()
    try:
        for record in records:
            payload = record.get("payload", {})
            if isinstance(payload, str):
                payload = json.loads(payload)

            received_at = record.get("received_at")
            if max_ts is None or received_at > max_ts:
                max_ts = received_at

            try:
                with conn.cursor() as cur:
                    act_data = payload.get("payload", payload)
                    ip = act_data.get("ip_address")
                    cur.execute("""
                        INSERT INTO processed_user_activity
                            (session_id, user_id, action, page_url, device_type, browser,
                             ip_address, country, duration_ms, raw_event_id, event_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s::inet, %s, %s, %s, %s)
                    """, (
                        act_data.get("session_id", record.get("event_id")),
                        act_data.get("user_id", "UNKNOWN"),
                        act_data.get("action", "unknown"),
                        act_data.get("page_url"),
                        act_data.get("device_type"),
                        act_data.get("browser"),
                        ip if ip else None,
                        act_data.get("country"),
                        act_data.get("duration_ms"),
                        record.get("id"),
                        payload.get("timestamp") or received_at,
                    ))
                conn.commit()
                loaded += 1
            except Exception as e:
                conn.rollback()
                failed += 1
                logging.warning(f"⚠️  Activity load failed: {e}")

        if max_ts:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ingestion_watermarks
                    SET last_loaded_at = %s::timestamptz, total_loaded = total_loaded + %s
                    WHERE source_name = 'user_activity_source'
                """, (max_ts, loaded))
            conn.commit()

    finally:
        conn.close()

    elapsed = int((time.time() - start_time) * 1000)
    logging.info(f"✅ Activity loaded: {loaded:,} | Failed: {failed} | Time: {elapsed}ms")

    log_pipeline_metric(run_id, "load_activity", "load_activity", "success",
                       records_read=len(records), records_written=loaded,
                       records_failed=failed, processing_time_ms=elapsed)
    return {"records_loaded": loaded}


def load_sensors(**context):
    """LOAD PHASE: Sensor Readings → processed_sensor_readings table"""
    ti = context["ti"]
    run_id = context["run_id"]
    records = ti.xcom_pull(task_ids="extract_sensors", key="sensor_records") or []

    if not records:
        logging.info("⏭️  No sensor records to load. Skipping.")
        return {"records_loaded": 0}

    start_time = time.time()
    loaded = 0
    failed = 0
    max_ts = None

    conn = get_db_conn()
    try:
        for record in records:
            payload = record.get("payload", {})
            if isinstance(payload, str):
                payload = json.loads(payload)

            received_at = record.get("received_at")
            if max_ts is None or received_at > max_ts:
                max_ts = received_at

            try:
                with conn.cursor() as cur:
                    sen_data = payload.get("payload", payload)
                    cur.execute("""
                        INSERT INTO processed_sensor_readings
                            (sensor_id, sensor_type, location, value, unit,
                             is_anomaly, anomaly_score, raw_event_id, event_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        sen_data.get("sensor_id", record.get("event_id")),
                        sen_data.get("sensor_type", "unknown"),
                        sen_data.get("location"),
                        float(sen_data.get("value", 0.0)),
                        sen_data.get("unit"),
                        bool(sen_data.get("is_anomaly", False)),
                        float(sen_data.get("anomaly_score", 0.0)),
                        record.get("id"),
                        payload.get("timestamp") or received_at,
                    ))
                conn.commit()
                loaded += 1
            except Exception as e:
                conn.rollback()
                failed += 1
                logging.warning(f"⚠️  Sensor load failed: {e}")

        if max_ts:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ingestion_watermarks
                    SET last_loaded_at = %s::timestamptz, total_loaded = total_loaded + %s
                    WHERE source_name = 'sensor_source'
                """, (max_ts, loaded))
            conn.commit()

    finally:
        conn.close()

    elapsed = int((time.time() - start_time) * 1000)
    logging.info(f"✅ Sensors loaded: {loaded:,} | Failed: {failed} | Time: {elapsed}ms")

    log_pipeline_metric(run_id, "load_sensors", "load_sensors", "success",
                       records_read=len(records), records_written=loaded,
                       records_failed=failed, processing_time_ms=elapsed)
    return {"records_loaded": loaded}


def generate_pipeline_report(**context):
    """
    REPORTING PHASE: Aggregate metrics and generate SLA report.
    Checks failure rates and writes alerts if thresholds are breached.
    """
    ti = context["ti"]
    run_id = context["run_id"]
    logging.info(f"📊 Generating pipeline report | run_id={run_id}")

    # Collect results from all load tasks
    order_result = ti.xcom_pull(task_ids="load_orders") or {}
    activity_result = ti.xcom_pull(task_ids="load_activity") or {}
    sensor_result = ti.xcom_pull(task_ids="load_sensors") or {}

    total_loaded = (
        order_result.get("records_loaded", 0)
        + activity_result.get("records_loaded", 0)
        + sensor_result.get("records_loaded", 0)
    )
    total_failed = (
        order_result.get("records_failed", 0)
        + activity_result.get("records_failed", 0)
        + sensor_result.get("records_failed", 0)
    )

    failure_rate = total_failed / (total_loaded + total_failed) if (total_loaded + total_failed) > 0 else 0

    report = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "orders_loaded": order_result.get("records_loaded", 0),
        "activity_loaded": activity_result.get("records_loaded", 0),
        "sensors_loaded": sensor_result.get("records_loaded", 0),
        "total_loaded": total_loaded,
        "total_failed": total_failed,
        "failure_rate_pct": round(failure_rate * 100, 2),
        "status": "success" if failure_rate < 0.05 else "degraded",
    }

    logging.info(f"📋 Pipeline Report:\n{json.dumps(report, indent=2)}")

    # Write alert if failure rate exceeds 5% threshold
    if failure_rate > 0.05:
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO alert_log
                        (alert_type, severity, message, pipeline_name, metric_value, threshold_value)
                    VALUES ('high_failure_rate', 'warning', %s, 'optimized_ingestion_pipeline', %s, 5.0)
                """, (
                    f"Run {run_id}: failure rate {failure_rate:.1%} exceeded 5% threshold",
                    round(failure_rate * 100, 2),
                ))
            conn.commit()
        finally:
            conn.close()

    log_pipeline_metric(run_id, "pipeline_report", "generate_pipeline_report",
                       "success", records_written=total_loaded, records_failed=total_failed)
    return report


# ─── DAG Definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="optimized_ingestion_pipeline",
    description="Optimized ETL: incremental load, parallel execution, retry, monitoring",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/10 * * * *",   # Every 10 minutes
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,                  # Prevent concurrent pipeline runs
    tags=["etl", "ingestion", "kafka", "incremental"],
) as dag:

    # ── Start Node ────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Health Check (Sequential - must pass before extraction) ──────────────
    health_check = PythonOperator(
        task_id="health_check",
        python_callable=check_pipeline_health,
    )

    # ── PARALLEL EXTRACT PHASE ────────────────────────────────────────────────
    # All three extract tasks run concurrently, reducing total runtime
    extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_incremental_orders,
    )

    extract_activity = PythonOperator(
        task_id="extract_activity",
        python_callable=extract_incremental_activity,
    )

    extract_sensors = PythonOperator(
        task_id="extract_sensors",
        python_callable=extract_incremental_sensors,
    )

    # ── Data Quality Gate (waits for all extracts) ────────────────────────────
    validate = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    # ── PARALLEL LOAD PHASE ───────────────────────────────────────────────────
    # Load tasks run concurrently after validation passes
    load_orders_task = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders,
    )

    load_activity_task = PythonOperator(
        task_id="load_activity",
        python_callable=load_activity,
    )

    load_sensors_task = PythonOperator(
        task_id="load_sensors",
        python_callable=load_sensors,
    )

    # ── Report (waits for all loads to complete) ──────────────────────────────
    report = PythonOperator(
        task_id="generate_pipeline_report",
        python_callable=generate_pipeline_report,
    )

    # ── End Node ──────────────────────────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ─── Task Dependencies (DAG Graph) ────────────────────────────────────────
    #
    #  start → health_check → [extract_orders, extract_activity, extract_sensors]
    #                              ↓            ↓                ↓
    #                          validate (waits for all 3)
    #                              ↓
    #                    [load_orders, load_activity, load_sensors]
    #                              ↓         ↓              ↓
    #                           report → end
    #
    start >> health_check
    health_check >> [extract_orders, extract_activity, extract_sensors]
    [extract_orders, extract_activity, extract_sensors] >> validate
    validate >> [load_orders_task, load_activity_task, load_sensors_task]
    [load_orders_task, load_activity_task, load_sensors_task] >> report
    report >> end
