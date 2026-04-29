#!/usr/bin/env python3
"""
Pipeline Demo Script
====================
Standalone script to verify the pipeline is working correctly.
Run this after docker-compose up to validate end-to-end data flow.

Usage:
    python demo.py
"""

import json
import time
import uuid
import random
import subprocess
from datetime import datetime, timezone


def print_banner():
    print("""
╔══════════════════════════════════════════════════════════════╗
║   🚀 Data Ingestion Pipeline - End-to-End Demo              ║
║   Based on: "Optimizing Data Ingestion Pipelines"           ║
╚══════════════════════════════════════════════════════════════╝
""")


def check_services():
    """Check all services are accessible."""
    import urllib.request
    services = {
        "FastAPI":     "http://localhost:8000/health",
        "Kafka UI":    "http://localhost:8080",
        "Airflow":     "http://localhost:8081/health",
    }
    print("🔍 Checking services...")
    for name, url in services.items():
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                print(f"  ✅ {name}: {url}")
        except Exception as e:
            print(f"  ❌ {name}: {e}")
    print()


def publish_test_events():
    """Publish test events via the API."""
    import urllib.request
    import urllib.error

    events = [
        {
            "event_type": "order",
            "payload": {
                "order_id": f"DEMO-{uuid.uuid4().hex[:8].upper()}",
                "customer_id": "CUST-DEMO001",
                "product_id": "LAPTOP-PRO-X1",
                "product_name": "Laptop Pro X1",
                "quantity": 2,
                "unit_price": 1299.99,
                "discount": 0.1,
                "total_amount": 2339.98,
                "status": "confirmed",
                "region": "US-EAST",
            },
            "count": 5,
        },
        {
            "event_type": "sensor",
            "payload": {
                "sensor_id": "SEN-9999",
                "sensor_type": "temperature",
                "location": "DataCenter-1",
                "value": 72.5,
                "unit": "°C",
                "is_anomaly": False,
            },
            "count": 10,
        },
    ]

    print("📤 Publishing test events to Kafka via API...")
    for event in events:
        data = json.dumps(event).encode()
        req = urllib.request.Request(
            "http://localhost:8000/events/publish",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                result = json.loads(r.read())
                print(f"  ✅ Published {result.get('events_published')} {event['event_type']} events")
        except Exception as e:
            print(f"  ❌ Failed: {e}")
    print()


def show_metrics():
    """Display pipeline metrics."""
    import urllib.request

    print("📊 Fetching pipeline metrics...")
    try:
        with urllib.request.urlopen("http://localhost:8000/metrics", timeout=10) as r:
            m = json.loads(r.read())
            print(f"""
  Raw Events:          {m.get('total_raw_events', 0):,}
  Processed Orders:    {m.get('total_processed_orders', 0):,}
  Processed Activity:  {m.get('total_processed_activity', 0):,}
  Processed Sensors:   {m.get('total_processed_sensors', 0):,}
  DLQ Events:          {m.get('total_dlq_events', 0):,}
  Open Alerts:         {m.get('total_open_alerts', 0):,}

  Watermarks:""")
            for w in m.get("watermarks", []):
                print(f"    {w['source_name']}: {w['last_loaded_at']} ({w['total_loaded']:,} loaded)")
    except Exception as e:
        print(f"  ❌ Could not fetch metrics: {e}")
    print()


def main():
    print_banner()
    print("⏳ Waiting 5s for services to be ready...")
    time.sleep(5)

    check_services()
    publish_test_events()

    print("⏳ Waiting 15s for consumer to process events...")
    time.sleep(15)

    show_metrics()

    print("""
✅ Demo complete! Access your services:
   - API Dashboard:  http://localhost:8000
   - API Docs:       http://localhost:8000/docs
   - Kafka UI:       http://localhost:8080
   - Airflow UI:     http://localhost:8081  (admin/admin)
""")


if __name__ == "__main__":
    main()
