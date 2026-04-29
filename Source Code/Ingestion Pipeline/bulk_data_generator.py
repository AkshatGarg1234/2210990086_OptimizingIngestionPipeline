#!/usr/bin/env python3
"""
Bulk Dummy Data Generator
=========================
This script generates a large volume of dummy data to stress-test the ingestion pipeline.
It hits the FastAPI control plane's `/events/publish` endpoint concurrently.

Usage:
    python bulk_data_generator.py --orders 5000 --activity 10000 --sensors 15000
"""

import argparse
import json
import urllib.request
import urllib.error
import concurrent.futures
import time
import uuid

API_URL = "http://localhost:8000/events/publish"
BATCH_SIZE = 500  # API handles up to 1000 per request, 500 is safe

def publish_batch(event_type: str, count: int, payload_template: dict) -> int:
    """Publish a batch of events via the API."""
    # We modify the template slightly for each batch to ensure uniqueness if needed,
    # though the API will generate unique event_ids automatically.
    
    # Update some fields to make the data look more varied even within the batch template
    if event_type == "order":
        payload_template["order_id"] = f"BULK-ORD-{uuid.uuid4().hex[:8].upper()}"
    elif event_type == "user_activity":
        payload_template["session_id"] = f"BULK-SES-{uuid.uuid4().hex[:8].upper()}"
    elif event_type == "sensor":
        payload_template["sensor_id"] = f"BULK-SEN-{uuid.uuid4().hex[:4].upper()}"

    data = {
        "event_type": event_type,
        "payload": payload_template,
        "count": count
    }
    
    req = urllib.request.Request(
        API_URL,
        data=json.dumps(data).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            response = json.loads(r.read())
            if response.get("success"):
                return response.get("events_published", 0)
    except urllib.error.URLError as e:
        print(f"❌ Error publishing {event_type} batch: {e}")
    except Exception as e:
        print(f"❌ Unexpected error publishing {event_type} batch: {e}")
        
    return 0

def get_templates():
    return {
        "order": {
            "customer_id": "CUST-BULK-01",
            "product_id": "LAPTOP-PRO-X1",
            "product_name": "Laptop Pro X1 Bulk",
            "quantity": 1,
            "unit_price": 1299.99,
            "discount": 0.0,
            "total_amount": 1299.99,
            "status": "pending",
            "region": "US-WEST",
        },
        "user_activity": {
            "user_id": "USER-BULK-01",
            "action": "view_product",
            "page_url": "https://example.com/products/laptop-pro-x1",
            "device_type": "desktop",
            "browser": "Chrome",
            "ip_address": "192.168.1.100",
            "country": "US",
            "duration_ms": 15000,
        },
        "sensor": {
            "sensor_type": "temperature",
            "location": "Warehouse-Bulk",
            "value": 22.5,
            "unit": "°C",
            "is_anomaly": False,
        }
    }

def generate_data(event_type: str, total_count: int, template: dict):
    if total_count <= 0:
        return

    print(f"🚀 Starting generation of {total_count:,} {event_type} events...")
    start_time = time.time()
    
    batches = []
    remaining = total_count
    while remaining > 0:
        current_batch = min(remaining, BATCH_SIZE)
        batches.append(current_batch)
        remaining -= current_batch

    total_published = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(publish_batch, event_type, batch_count, template) for batch_count in batches]
        for future in concurrent.futures.as_completed(futures):
            total_published += future.result()
            
    elapsed = time.time() - start_time
    rps = total_published / elapsed if elapsed > 0 else 0
    print(f"✅ Finished {event_type}: Published {total_published:,} events in {elapsed:.2f}s ({rps:.0f} req/s)")


def main():
    parser = argparse.ArgumentParser(description="Bulk Dummy Data Generator for Pipeline Testing")
    parser.add_argument("--orders", type=int, default=1000, help="Number of order events to generate (default: 1000)")
    parser.add_argument("--activity", type=int, default=2000, help="Number of user activity events to generate (default: 2000)")
    parser.add_argument("--sensors", type=int, default=5000, help="Number of sensor events to generate (default: 5000)")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("  BULK DUMMY DATA GENERATOR")
    print("=" * 60)
    print(f"Target API: {API_URL}")
    print(f"Requested: Orders={args.orders}, Activity={args.activity}, Sensors={args.sensors}")
    print("-" * 60)
    
    # Check if API is reachable
    try:
        urllib.request.urlopen("http://localhost:8000/health", timeout=5)
    except Exception as e:
        print(f"❌ Cannot reach API at localhost:8000. Is docker-compose running?\nError: {e}")
        return

    templates = get_templates()
    
    overall_start = time.time()
    
    generate_data("order", args.orders, templates["order"])
    generate_data("user_activity", args.activity, templates["user_activity"])
    generate_data("sensor", args.sensors, templates["sensor"])
    
    overall_elapsed = time.time() - overall_start
    total_events = args.orders + args.activity + args.sensors
    
    print("-" * 60)
    print(f"🎉 Bulk generation complete! Total time: {overall_elapsed:.2f}s")
    print(f"📊 Check the Airflow dashboard to trigger the pipeline and monitor processing:")
    print("   http://localhost:8081")
    print(f"📊 Check the API metrics endpoint to see the incoming data count:")
    print("   http://localhost:8000/metrics")

if __name__ == "__main__":
    main()
