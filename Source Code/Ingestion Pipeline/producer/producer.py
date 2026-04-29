"""
Data Producer Service
=====================
Simulates multiple data sources (orders, user activity, sensor readings)
and publishes events to Kafka topics in real time.

Architecture:
  - Runs 3 parallel producer threads (one per data source)
  - Each thread generates events at a configurable interval
  - Events are serialized as JSON and published to dedicated Kafka topics
  - Implements retry logic on Kafka connection failures
"""

import json
import logging
import os
import random
import threading
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PRODUCE_INTERVAL_SECONDS = float(os.getenv("PRODUCE_INTERVAL_SECONDS", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

TOPICS = {
    "orders": "raw.orders",
    "user_activity": "raw.user-activity",
    "sensors": "raw.sensor-readings",
}

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("producer")

fake = Faker()


# ─── Event Generators ─────────────────────────────────────────────────────────

def generate_order_event() -> dict:
    """Generate a realistic e-commerce order event."""
    products = [
        ("LAPTOP-PRO-X1", "Laptop Pro X1", 1299.99),
        ("WIRELESS-HEADPHONES", "Wireless Headphones", 199.99),
        ("SMART-WATCH-V2", "Smart Watch V2", 349.99),
        ("USB-C-HUB", "USB-C Hub 7-Port", 49.99),
        ("MECH-KEYBOARD", "Mechanical Keyboard", 129.99),
        ("4K-MONITOR", "4K Monitor 27inch", 499.99),
        ("GAMING-MOUSE", "Gaming Mouse RGB", 79.99),
        ("WEBCAM-HD", "Webcam Full HD", 89.99),
    ]
    product_id, product_name, unit_price = random.choice(products)
    quantity = random.randint(1, 10)
    discount = round(random.uniform(0, 0.2), 2)
    total = round(quantity * unit_price * (1 - discount), 2)

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "order",
        "source": "orders_source",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
            "customer_id": f"CUST-{fake.uuid4()[:8].upper()}",
            "product_id": product_id,
            "product_name": product_name,
            "quantity": quantity,
            "unit_price": unit_price,
            "discount": discount,
            "total_amount": total,
            "status": random.choice(["pending", "confirmed", "shipped", "delivered"]),
            "region": random.choice(["US-EAST", "US-WEST", "EU-CENTRAL", "APAC", "LATAM"]),
        },
    }


def generate_user_activity_event() -> dict:
    """Generate a realistic user activity / clickstream event."""
    actions = ["login", "view_product", "add_to_cart", "checkout", "purchase", "logout", "search"]
    pages = ["/home", "/products", "/cart", "/checkout", "/profile", "/search", "/orders"]

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "user_activity",
        "source": "user_activity_source",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "session_id": f"SES-{uuid.uuid4().hex[:16].upper()}",
            "user_id": f"USER-{fake.uuid4()[:8].upper()}",
            "action": random.choice(actions),
            "page_url": f"https://example.com{random.choice(pages)}",
            "device_type": random.choice(["desktop", "mobile", "tablet"]),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
            "ip_address": fake.ipv4(),
            "country": fake.country_code(),
            "duration_ms": random.randint(50, 30000),
        },
    }


def generate_sensor_event() -> dict:
    """Generate an IoT sensor reading event."""
    sensor_types = {
        "temperature": ("°C", 15.0, 85.0),
        "pressure": ("hPa", 900.0, 1100.0),
        "humidity": ("%", 10.0, 95.0),
        "voltage": ("V", 100.0, 240.0),
        "current": ("A", 0.5, 50.0),
    }
    sensor_type, (unit, min_val, max_val) = random.choice(list(sensor_types.items()))
    value = round(random.uniform(min_val, max_val), 4)

    # Simulate occasional anomalies (5% chance)
    is_anomaly = random.random() < 0.05
    if is_anomaly:
        value = round(value * random.uniform(1.5, 3.0), 4)  # Spike anomaly

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "sensor",
        "source": "sensor_source",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "sensor_id": f"SEN-{random.randint(1000, 9999)}",
            "sensor_type": sensor_type,
            "location": random.choice([
                "Warehouse-A", "Warehouse-B", "DataCenter-1",
                "Office-Floor-3", "Server-Room-A"
            ]),
            "value": value,
            "unit": unit,
            "is_anomaly": is_anomaly,
        },
    }


# ─── Kafka Producer Factory ────────────────────────────────────────────────────

def create_kafka_producer(max_retries: int = 10, retry_delay: int = 5) -> KafkaProducer:
    """
    Create a Kafka producer with retry logic.
    Retries until broker is available (handles startup ordering).
    """
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",                 # Wait for all replicas
                retries=3,                  # Internal Kafka retries
                max_in_flight_requests_per_connection=1,  # Preserve ordering
                compression_type="gzip",    # Reduce network bandwidth
                batch_size=16384,           # 16KB batch
                linger_ms=10,              # Wait 10ms to batch messages
            )
            logger.info(f"✅ Kafka producer connected on attempt {attempt}")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"⏳ Kafka not ready (attempt {attempt}/{max_retries}). Retrying in {retry_delay}s...")
            time.sleep(retry_delay)

    raise RuntimeError("❌ Could not connect to Kafka after max retries")


def on_send_success(record_metadata, event_type: str):
    """Callback on successful Kafka send."""
    logger.debug(
        f"[{event_type}] → topic={record_metadata.topic} "
        f"partition={record_metadata.partition} offset={record_metadata.offset}"
    )


def on_send_error(excp, event_type: str):
    """Callback on Kafka send failure."""
    logger.error(f"[{event_type}] ❌ Failed to publish: {excp}")


# ─── Producer Worker ──────────────────────────────────────────────────────────

def producer_worker(
    name: str,
    topic: str,
    generator_fn,
    producer: KafkaProducer,
    interval: float,
    batch_size: int,
):
    """
    Worker function that continuously produces events to a Kafka topic.
    Sends events in configurable batch sizes at fixed intervals.
    """
    logger.info(f"🚀 [{name}] Starting producer → topic: {topic}")
    events_sent = 0

    while True:
        try:
            batch = [generator_fn() for _ in range(batch_size)]
            for event in batch:
                # Use event_id as Kafka message key for partitioning
                key = event["event_id"]
                producer.send(
                    topic,
                    key=key,
                    value=event,
                ).add_callback(
                    lambda meta: on_send_success(meta, name)
                ).add_errback(
                    lambda exc: on_send_error(exc, name)
                )

            producer.flush()
            events_sent += batch_size
            logger.info(
                f"📤 [{name}] Sent batch of {batch_size} | "
                f"Total: {events_sent:,} | Topic: {topic}"
            )

        except KafkaError as e:
            logger.error(f"[{name}] ❌ Kafka error: {e}")
        except Exception as e:
            logger.error(f"[{name}] ❌ Unexpected error: {e}")

        time.sleep(interval)


# ─── Entry Point ──────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("  DATA INGESTION PIPELINE - PRODUCER SERVICE")
    logger.info(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"  Interval: {PRODUCE_INTERVAL_SECONDS}s | Batch: {BATCH_SIZE}")
    logger.info("=" * 60)

    # Wait for Kafka to be fully ready
    time.sleep(10)

    producer = create_kafka_producer()

    # Define producer threads: one per data source
    workers = [
        {
            "name": "Orders-Producer",
            "topic": TOPICS["orders"],
            "generator_fn": generate_order_event,
        },
        {
            "name": "UserActivity-Producer",
            "topic": TOPICS["user_activity"],
            "generator_fn": generate_user_activity_event,
        },
        {
            "name": "Sensor-Producer",
            "topic": TOPICS["sensors"],
            "generator_fn": generate_sensor_event,
        },
    ]

    threads = []
    for w in workers:
        t = threading.Thread(
            target=producer_worker,
            kwargs={
                "name": w["name"],
                "topic": w["topic"],
                "generator_fn": w["generator_fn"],
                "producer": producer,
                "interval": PRODUCE_INTERVAL_SECONDS,
                "batch_size": BATCH_SIZE,
            },
            daemon=True,
            name=w["name"],
        )
        threads.append(t)
        t.start()
        logger.info(f"🧵 Thread started: {w['name']}")

    # Keep main thread alive
    try:
        while True:
            time.sleep(30)
            alive = sum(1 for t in threads if t.is_alive())
            logger.info(f"💓 Heartbeat | Active threads: {alive}/{len(threads)}")
    except KeyboardInterrupt:
        logger.info("🛑 Producer shutting down...")
        producer.close()


if __name__ == "__main__":
    main()
