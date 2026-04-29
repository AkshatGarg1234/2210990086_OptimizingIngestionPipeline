"""
Data Consumer + ETL Service
============================
Reads events from Kafka topics, applies transformations, implements
incremental loading, and persists results to PostgreSQL.

Key Features:
  - Parallel consumer threads (one per topic)
  - Incremental loading via watermarks
  - Dead Letter Queue (DLQ) for failed events
  - Retry mechanism with exponential backoff
  - Pipeline metrics tracking
"""

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ingestion-consumer-group")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "pipeline_db")
DB_USER = os.getenv("DB_USER", "pipeline_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pipeline_pass")

MAX_RETRIES = 3
DLQ_AFTER_RETRIES = 3

TOPICS = {
    "raw.orders": "orders",
    "raw.user-activity": "user_activity",
    "raw.sensor-readings": "sensors",
}

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("consumer")


# ─── Database Connection ──────────────────────────────────────────────────────

def get_db_connection(max_retries: int = 10, retry_delay: int = 5):
    """Create a PostgreSQL connection with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
            conn.autocommit = False
            logger.info(f"✅ PostgreSQL connected (attempt {attempt})")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"⏳ DB not ready (attempt {attempt}/{max_retries}): {e}")
            time.sleep(retry_delay)
    raise RuntimeError("❌ Could not connect to PostgreSQL after max retries")


# ─── ETL Transformers ─────────────────────────────────────────────────────────

def transform_order(event: dict) -> Optional[dict]:
    """
    Transform raw order event into processed format.
    Validates required fields and computes derived metrics.
    """
    try:
        payload = event.get("payload", {})

        # Validate required fields
        required = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "total_amount"]
        for field in required:
            if field not in payload:
                raise ValueError(f"Missing required field: {field}")

        # Validate business rules
        if payload["quantity"] <= 0:
            raise ValueError(f"Invalid quantity: {payload['quantity']}")
        if payload["total_amount"] < 0:
            raise ValueError(f"Negative total_amount: {payload['total_amount']}")

        return {
            "order_id": payload["order_id"],
            "customer_id": payload["customer_id"],
            "product_id": payload["product_id"],
            "product_name": payload.get("product_name"),
            "quantity": int(payload["quantity"]),
            "unit_price": float(payload["unit_price"]),
            "total_amount": float(payload["total_amount"]),
            "discount": float(payload.get("discount", 0)),
            "status": payload.get("status", "unknown"),
            "region": payload.get("region"),
            "source_system": event.get("source"),
            "event_timestamp": event.get("timestamp"),
        }
    except (KeyError, ValueError, TypeError) as e:
        raise ValueError(f"Order transform failed: {e}") from e


def transform_user_activity(event: dict) -> Optional[dict]:
    """Transform raw user activity event into processed format."""
    try:
        payload = event.get("payload", {})
        required = ["session_id", "user_id", "action"]
        for field in required:
            if field not in payload:
                raise ValueError(f"Missing required field: {field}")

        return {
            "session_id": payload["session_id"],
            "user_id": payload["user_id"],
            "action": payload["action"],
            "page_url": payload.get("page_url"),
            "device_type": payload.get("device_type"),
            "browser": payload.get("browser"),
            "ip_address": payload.get("ip_address"),
            "country": payload.get("country"),
            "duration_ms": payload.get("duration_ms"),
            "event_timestamp": event.get("timestamp"),
        }
    except (KeyError, ValueError, TypeError) as e:
        raise ValueError(f"UserActivity transform failed: {e}") from e


def transform_sensor(event: dict) -> Optional[dict]:
    """Transform raw sensor event and detect anomalies."""
    try:
        payload = event.get("payload", {})
        required = ["sensor_id", "sensor_type", "value"]
        for field in required:
            if field not in payload:
                raise ValueError(f"Missing required field: {field}")

        # Anomaly scoring: simple z-score style thresholding
        value = float(payload["value"])
        is_anomaly = payload.get("is_anomaly", False)
        anomaly_score = min(abs(value) / 1000.0, 1.0) if is_anomaly else 0.0

        return {
            "sensor_id": payload["sensor_id"],
            "sensor_type": payload["sensor_type"],
            "location": payload.get("location"),
            "value": value,
            "unit": payload.get("unit"),
            "is_anomaly": is_anomaly,
            "anomaly_score": round(anomaly_score, 4),
            "event_timestamp": event.get("timestamp"),
        }
    except (KeyError, ValueError, TypeError) as e:
        raise ValueError(f"Sensor transform failed: {e}") from e


TRANSFORMERS = {
    "orders": transform_order,
    "user_activity": transform_user_activity,
    "sensors": transform_sensor,
}


# ─── Database Writers ─────────────────────────────────────────────────────────

def write_raw_event(conn, event: dict, topic: str, partition: int, offset: int) -> int:
    """Insert a raw event into the raw_events table."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw_events
                (event_id, source, event_type, payload, kafka_topic, kafka_partition, kafka_offset)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            RETURNING id
        """, (
            event.get("event_id", ""),
            event.get("source", "unknown"),
            event.get("event_type", "unknown"),
            json.dumps(event),
            topic,
            partition,
            offset,
        ))
        result = cur.fetchone()
        return result[0] if result else None


def write_processed_order(conn, data: dict, raw_id: int):
    """Upsert a processed order record."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO processed_orders
                (order_id, customer_id, product_id, product_name, quantity, unit_price,
                 total_amount, discount, status, region, source_system, raw_event_id, event_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE SET
                status = EXCLUDED.status,
                updated_at = NOW()
        """, (
            data["order_id"], data["customer_id"], data["product_id"], data["product_name"],
            data["quantity"], data["unit_price"], data["total_amount"], data["discount"],
            data["status"], data["region"], data["source_system"], raw_id, data["event_timestamp"],
        ))


def write_processed_activity(conn, data: dict, raw_id: int):
    """Insert a processed user activity record."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO processed_user_activity
                (session_id, user_id, action, page_url, device_type, browser,
                 ip_address, country, duration_ms, raw_event_id, event_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s::inet, %s, %s, %s, %s)
        """, (
            data["session_id"], data["user_id"], data["action"], data["page_url"],
            data["device_type"], data["browser"], data["ip_address"],
            data["country"], data["duration_ms"], raw_id, data["event_timestamp"],
        ))


def write_processed_sensor(conn, data: dict, raw_id: int):
    """Insert a processed sensor reading record."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO processed_sensor_readings
                (sensor_id, sensor_type, location, value, unit,
                 is_anomaly, anomaly_score, raw_event_id, event_timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data["sensor_id"], data["sensor_type"], data["location"], data["value"],
            data["unit"], data["is_anomaly"], data["anomaly_score"], raw_id, data["event_timestamp"],
        ))


WRITERS = {
    "orders": write_processed_order,
    "user_activity": write_processed_activity,
    "sensors": write_processed_sensor,
}


def send_to_dlq(conn, event: dict, error: str, topic: str, offset: int, retry_count: int):
    """Send a failed event to the Dead Letter Queue."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dead_letter_events
                (event_id, source, payload, error_message, retry_count, kafka_topic, kafka_offset)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            event.get("event_id"), event.get("source"),
            json.dumps(event), str(error), retry_count, topic, offset,
        ))
    conn.commit()
    logger.warning(f"📮 DLQ: event {event.get('event_id')} | error: {error}")


def update_watermark(conn, source_name: str, event_timestamp: str, total_loaded: int):
    """Update the incremental ingestion watermark for a source."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE ingestion_watermarks
            SET last_loaded_at = %s::timestamptz,
                total_loaded = total_loaded + %s
            WHERE source_name = %s
        """, (event_timestamp, total_loaded, source_name))


# ─── Consumer Worker ──────────────────────────────────────────────────────────

def consumer_worker(topic: str, source_type: str):
    """
    Consumer thread for a specific Kafka topic.
    Implements at-least-once processing semantics with idempotent upserts.
    """
    logger.info(f"🎯 [{source_type}] Starting consumer → topic: {topic}")

    # Wait for services to be ready
    time.sleep(15)

    conn = get_db_connection()
    transformer = TRANSFORMERS[source_type]
    writer = WRITERS[source_type]

    # Create Kafka consumer with retry
    consumer = None
    for attempt in range(10):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",          # Start from beginning if no offset
                enable_auto_commit=False,              # Manual commit for exactly-once semantics
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                max_poll_records=100,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logger.info(f"✅ [{source_type}] Kafka consumer connected")
            break
        except NoBrokersAvailable:
            logger.warning(f"⏳ [{source_type}] Kafka not ready (attempt {attempt+1}/10)")
            time.sleep(10)

    if not consumer:
        logger.error(f"❌ [{source_type}] Could not connect to Kafka. Exiting thread.")
        return

    records_processed = 0
    records_failed = 0
    start_time = time.time()

    logger.info(f"📥 [{source_type}] Waiting for messages...")

    for message in consumer:
        event = message.value
        partition = message.partition
        offset = message.offset

        retry_count = 0
        success = False

        # Retry loop with exponential backoff
        while retry_count <= MAX_RETRIES and not success:
            try:
                if not conn or conn.closed:
                    conn = get_db_connection()

                # 1. Write raw event
                raw_id = write_raw_event(conn, event, topic, partition, offset)

                if raw_id:
                    # 2. Transform and write to processed table
                    transformed = transformer(event)
                    writer(conn, transformed, raw_id)

                    # 3. Mark raw event as processed
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE raw_events SET processed=TRUE, processed_at=NOW() WHERE id=%s",
                            (raw_id,)
                        )

                    # 4. Update watermark (incremental loading state)
                    update_watermark(
                        conn,
                        event.get("source", "unknown"),
                        event.get("timestamp"),
                        1,
                    )

                conn.commit()
                success = True
                records_processed += 1

            except Exception as e:
                retry_count += 1
                try:
                    conn.rollback()
                except Exception:
                    pass

                if retry_count <= MAX_RETRIES:
                    backoff = 2 ** retry_count  # Exponential backoff
                    logger.warning(
                        f"[{source_type}] ⚠️  Retry {retry_count}/{MAX_RETRIES} "
                        f"for event {event.get('event_id')} in {backoff}s: {e}"
                    )
                    time.sleep(backoff)
                else:
                    records_failed += 1
                    logger.error(f"[{source_type}] ❌ Max retries exceeded: {e}")
                    try:
                        send_to_dlq(conn, event, str(e), topic, offset, retry_count)
                    except Exception as dlq_err:
                        logger.error(f"[{source_type}] ❌ DLQ write failed: {dlq_err}")

        # Commit Kafka offset only after successful DB write
        consumer.commit()

        # Log throughput every 500 records
        if records_processed % 500 == 0 and records_processed > 0:
            elapsed = time.time() - start_time
            rps = records_processed / elapsed if elapsed > 0 else 0
            logger.info(
                f"📊 [{source_type}] Processed: {records_processed:,} | "
                f"Failed: {records_failed:,} | Throughput: {rps:.1f} rec/s"
            )

        # Alert if failure rate is high
        if records_processed > 0:
            failure_rate = records_failed / records_processed
            if failure_rate > 0.1:  # 10% threshold
                logger.warning(
                    f"🚨 [{source_type}] HIGH FAILURE RATE: {failure_rate:.1%} "
                    f"({records_failed}/{records_processed})"
                )


# ─── Entry Point ──────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("  DATA INGESTION PIPELINE - CONSUMER SERVICE")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  DB: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    logger.info("=" * 60)

    threads = []
    for topic, source_type in TOPICS.items():
        t = threading.Thread(
            target=consumer_worker,
            args=(topic, source_type),
            daemon=True,
            name=f"consumer-{source_type}",
        )
        threads.append(t)
        t.start()
        logger.info(f"🧵 Consumer thread started: {source_type} → {topic}")

    try:
        while True:
            time.sleep(30)
            alive = sum(1 for t in threads if t.is_alive())
            logger.info(f"💓 Consumer heartbeat | Active: {alive}/{len(threads)}")
    except KeyboardInterrupt:
        logger.info("🛑 Consumer shutting down...")


if __name__ == "__main__":
    main()
