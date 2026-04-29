# 🚀 Optimized Data Ingestion Pipeline

> A **production-grade, fully dockerized ETL pipeline** implementing the research paper:
> **"Optimizing Data Ingestion Pipelines for Efficient Big Data Processing"**

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (Simulated)                             │
│     Orders Source     │   User Activity Source    │    Sensor Source        │
└───────────┬───────────┴────────────┬──────────────┴───────────┬─────────────┘
            │                        │                            │
            ▼                        ▼                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    KAFKA EVENT STREAMING LAYER                              │
│   raw.orders (3 partitions) │ raw.user-activity │ raw.sensor-readings      │
│                         Zookeeper + Kafka                                   │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONSUMER + ETL LAYER (Python)                            │
│   - Parallel consumers (3 threads)                                          │
│   - Transform & validate events                                             │
│   - Incremental watermark tracking                                          │
│   - Retry with exponential backoff                                          │
│   - Dead Letter Queue for failed events                                     │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                   STORAGE LAYER (PostgreSQL)                                │
│   raw_events │ processed_orders │ processed_user_activity │ sensor_readings  │
│   pipeline_metrics │ ingestion_watermarks │ dead_letter_events │ alert_log   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │
┌─────────────────────────────────────────────────────────────────────────────┐
│              ORCHESTRATION LAYER (Apache Airflow)                           │
│                                                                             │
│  start → health_check → [extract_orders ║ extract_activity ║ extract_sensors]
│                                ↓              ↓                   ↓
│                         validate_data_quality (quality gate)
│                                ↓
│                   [load_orders ║ load_activity ║ load_sensors]  (parallel)
│                                ↓              ↓          ↓
│                         generate_pipeline_report → end
│                                                                             │
│  Schedule: every 10 minutes │ Retry: 3x with exponential backoff            │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONTROL PLANE (FastAPI)                                  │
│   /health  │  /metrics  │  /pipeline/trigger  │  /events/publish           │
│   /pipeline/dlq  │  /pipeline/alerts  │  Dashboard UI                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📦 Project Structure

```
Ingestion Pipeline/
├── docker-compose.yml          # Orchestrates all services
├── demo.py                     # End-to-end validation script
├── README.md
│
├── airflow/
│   ├── dags/
│   │   └── ingestion_pipeline.py   # Optimized parallel DAG
│   ├── logs/                       # Airflow task logs
│   └── plugins/                    # Shared ETL plugins
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py             # Multi-source Kafka event producer
│
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── consumer.py             # Kafka → PostgreSQL ETL consumer
│
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                 # FastAPI control plane
│
├── etl/
│   └── __init__.py             # Shared ETL utilities
│
├── db/
│   └── init.sql                # Full schema initialization
│
└── shared/                     # Shared volume between services
```

---

## ⚡ Quick Start

### Prerequisites
- **Docker** (https://docs.docker.com/get-docker/)
- **Docker Compose** (included with Docker Desktop)

### Step 1 — Clone and Launch

```bash
cd "Ingestion Pipeline"
docker-compose up --build
```

> First build takes ~3-5 minutes (downloads images, installs dependencies).

### Step 2 — Access Services

| Service        | URL                          | Credentials     |
|----------------|------------------------------|-----------------|
| **Pipeline API** | http://localhost:8000      | —               |
| **API Docs**     | http://localhost:8000/docs | —               |
| **Kafka UI**     | http://localhost:8080      | —               |
| **Airflow UI**   | http://localhost:8081      | admin / admin   |
| **PostgreSQL**   | localhost:5432             | pipeline_user / pipeline_pass |

### Step 3 — Run Demo

In a new terminal:

```bash
python demo.py
```

Or trigger via the API:

```bash
curl -X POST http://localhost:8000/pipeline/trigger
```

---

## 🔄 Optimized Pipeline Features

### 1. Incremental Ingestion
The DAG uses **watermark-based incremental loading** — instead of re-processing all data, it reads the `ingestion_watermarks` table to find the last successful load timestamp and only fetches newer records.

```sql
-- Only fetch records newer than the last watermark
SELECT * FROM raw_events
WHERE source = 'orders_source'
  AND received_at > (SELECT last_loaded_at FROM ingestion_watermarks WHERE source_name = 'orders_source')
ORDER BY received_at ASC
LIMIT 10000;
```

### 2. Parallel Task Execution
The Airflow DAG runs extract and load phases **in parallel** across 3 data sources simultaneously, cutting total runtime by ~3x.

```
Sequential (Baseline):  Extract1 → Extract2 → Extract3 → Load1 → Load2 → Load3  (~60s)
Optimized:             [Extract1 ║ Extract2 ║ Extract3] → [Load1 ║ Load2 ║ Load3]  (~20s)
```

### 3. Fault Tolerance
- **Consumer**: Per-event retry loop with exponential backoff (2^n seconds)
- **Airflow DAG**: 3 retries with exponential backoff, 10-minute max delay
- **Dead Letter Queue**: Failed events stored in `dead_letter_events` table for analysis
- **Kafka offsets**: Only committed after successful DB write (at-least-once)

### 4. Event-Driven Ingestion
```
Producer → Kafka (raw.orders / raw.user-activity / raw.sensor-readings)
            ↓
Consumer reads in real-time, transforms, writes to PostgreSQL
            ↓  
Airflow runs every 10 minutes for additional batch validation and reporting
```

---

## 📊 Monitoring & Metrics

### Via API
```bash
curl http://localhost:8000/metrics    # Full statistics
curl http://localhost:8000/health     # Service health
curl http://localhost:8000/pipeline/dlq     # Failed events
curl http://localhost:8000/pipeline/alerts  # Pipeline alerts
```

### Via Airflow
1. Open http://localhost:8081
2. Login: `admin` / `admin`
3. Find DAG: `optimized_ingestion_pipeline`
4. View task logs, timing, and retry history

### Via Database
```sql
-- Throughput overview
SELECT pipeline_name, AVG(throughput_rps) AS avg_rps,
       SUM(records_written) AS total_written
FROM pipeline_metrics
GROUP BY pipeline_name;

-- Check watermarks
SELECT * FROM ingestion_watermarks;

-- View DLQ
SELECT * FROM dead_letter_events ORDER BY failed_at DESC;
```

---

## 🛠️ Common Operations

### Trigger full reload (reset watermarks)
```bash
curl -X DELETE http://localhost:8000/pipeline/watermarks/reset
```

### Publish custom events
```bash
curl -X POST http://localhost:8000/events/publish \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "order",
    "payload": {"order_id": "MY-001", "customer_id": "C001", "total_amount": 999.99},
    "count": 10
  }'
```

### Generate Bulk Data for Testing
Use the bulk data generator script to rapidly pump thousands of records into the pipeline for stress-testing and performance validation:
```bash
python bulk_data_generator.py --orders 5000 --activity 10000 --sensors 15000
```
This script runs concurrently and publishes events via the FastAPI endpoint.

### Stop the pipeline
```bash
docker-compose down
```

### Stop and remove all data (fresh start)
```bash
docker-compose down -v
```

---

## 🔬 Evaluation Metrics (Research Paper)

| Metric | How to Measure |
|--------|---------------|
| **Processing Time** | `pipeline_metrics.processing_time_ms` |
| **Throughput (rec/s)** | `pipeline_metrics.throughput_rps` |
| **Failure Rate** | `records_failed / (records_read + records_failed)` |
| **Retry Success Rate** | `dead_letter_events vs total_events` ratio |
| **Resource Utilization** | `docker stats` command |

---

## 🔮 Extending the System

### Add a new data source
1. Add a generator in `producer/producer.py`
2. Add a new Kafka topic to `TOPICS` dict
3. Add a transformer and writer in `consumer/consumer.py`
4. Add Extract + Load tasks to the Airflow DAG

### Enable real email alerts
Set `email_on_failure: True` in DAG args and configure SMTP in `docker-compose.yml`:
```yaml
AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
AIRFLOW__SMTP__SMTP_USER: your@email.com
AIRFLOW__SMTP__SMTP_PASSWORD: your-app-password
```

### Scale the consumer
```yaml
consumer:
  deploy:
    replicas: 3  # Run 3 consumer instances
```

---

## 📚 Research Paper Concepts Implemented

| Concept | Implementation |
|---------|---------------|
| Traditional ETL | Sequential baseline (comments in DAG) |
| Incremental Loading | `ingestion_watermarks` table + watermark queries |
| Parallel Execution | Airflow parallel task branches |
| Event-Driven Ingestion | Kafka → Consumer service (real-time) |
| Workflow Orchestration | Apache Airflow DAG with scheduling |
| Fault Tolerance | Retry loop, DLQ, exponential backoff |
| Monitoring | `pipeline_metrics` table, alert_log, FastAPI |

---

## 🐛 Troubleshooting

**Kafka not connecting?** Wait 60s after `docker-compose up` for Kafka to fully initialize.

**Airflow DAG not appearing?** Check `docker logs pipeline-airflow-scheduler` for Python errors in the DAG file.

**DB connection refused?** Wait for `pipeline-postgres` container to show `healthy` status: `docker ps`.

**Port conflict?** Change port mappings in `docker-compose.yml` (e.g., `8000:8000` → `8001:8000`).
