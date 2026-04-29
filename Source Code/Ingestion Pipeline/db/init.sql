-- =============================================================================
-- Data Ingestion Pipeline - Database Initialization Script
-- =============================================================================

-- Create pipeline database and user
CREATE USER pipeline_user WITH PASSWORD 'pipeline_pass';
CREATE DATABASE pipeline_db;
GRANT ALL PRIVILEGES ON DATABASE pipeline_db TO pipeline_user;

-- Create airflow database for Airflow metadata
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- Switch to pipeline database
\c pipeline_db;

-- Grant schema access to pipeline_user
GRANT ALL ON SCHEMA public TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO pipeline_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO pipeline_user;

-- =============================================================================
-- RAW DATA TABLE
-- Stores raw ingested events exactly as received from Kafka
-- =============================================================================
CREATE TABLE IF NOT EXISTS raw_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(64) UNIQUE NOT NULL,
    source          VARCHAR(100) NOT NULL,         -- Source system identifier
    event_type      VARCHAR(50) NOT NULL,           -- e.g. 'order', 'user_activity', 'sensor'
    payload         JSONB NOT NULL,                 -- Full raw event payload
    kafka_topic     VARCHAR(200),
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed       BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at    TIMESTAMPTZ,
    retry_count     INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX idx_raw_events_received_at ON raw_events(received_at);
CREATE INDEX idx_raw_events_processed ON raw_events(processed);
CREATE INDEX idx_raw_events_source ON raw_events(source);
CREATE INDEX idx_raw_events_type ON raw_events(event_type);
CREATE INDEX idx_raw_events_payload ON raw_events USING GIN(payload);

-- =============================================================================
-- PROCESSED ORDERS TABLE
-- Transformed and validated order events
-- =============================================================================
CREATE TABLE IF NOT EXISTS processed_orders (
    id              BIGSERIAL PRIMARY KEY,
    order_id        VARCHAR(64) UNIQUE NOT NULL,
    customer_id     VARCHAR(64) NOT NULL,
    product_id      VARCHAR(64) NOT NULL,
    product_name    VARCHAR(255),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(12, 2) NOT NULL,
    total_amount    NUMERIC(12, 2) NOT NULL,
    discount        NUMERIC(5, 2) DEFAULT 0,
    status          VARCHAR(50) NOT NULL,          -- pending, confirmed, shipped, delivered
    region          VARCHAR(100),
    source_system   VARCHAR(100),
    raw_event_id    BIGINT REFERENCES raw_events(id),
    event_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_orders_customer ON processed_orders(customer_id);
CREATE INDEX idx_orders_event_time ON processed_orders(event_timestamp);
CREATE INDEX idx_orders_ingested ON processed_orders(ingested_at);
CREATE INDEX idx_orders_status ON processed_orders(status);
CREATE INDEX idx_orders_region ON processed_orders(region);

-- =============================================================================
-- USER ACTIVITY TABLE
-- Transformed user behavior / clickstream events
-- =============================================================================
CREATE TABLE IF NOT EXISTS processed_user_activity (
    id              BIGSERIAL PRIMARY KEY,
    session_id      VARCHAR(64) NOT NULL,
    user_id         VARCHAR(64) NOT NULL,
    action          VARCHAR(100) NOT NULL,          -- login, click, purchase, logout
    page_url        VARCHAR(500),
    device_type     VARCHAR(50),
    browser         VARCHAR(100),
    ip_address      INET,
    country         VARCHAR(100),
    duration_ms     INTEGER,
    raw_event_id    BIGINT REFERENCES raw_events(id),
    event_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_activity_user ON processed_user_activity(user_id);
CREATE INDEX idx_activity_time ON processed_user_activity(event_timestamp);
CREATE INDEX idx_activity_action ON processed_user_activity(action);

-- =============================================================================
-- SENSOR READINGS TABLE
-- IoT/sensor data stream events
-- =============================================================================
CREATE TABLE IF NOT EXISTS processed_sensor_readings (
    id              BIGSERIAL PRIMARY KEY,
    sensor_id       VARCHAR(64) NOT NULL,
    sensor_type     VARCHAR(100) NOT NULL,          -- temperature, pressure, humidity
    location        VARCHAR(200),
    value           NUMERIC(15, 4) NOT NULL,
    unit            VARCHAR(20),
    is_anomaly      BOOLEAN DEFAULT FALSE,
    anomaly_score   NUMERIC(5, 4),
    raw_event_id    BIGINT REFERENCES raw_events(id),
    event_timestamp TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sensor_id ON processed_sensor_readings(sensor_id);
CREATE INDEX idx_sensor_time ON processed_sensor_readings(event_timestamp);
CREATE INDEX idx_sensor_anomaly ON processed_sensor_readings(is_anomaly);

-- =============================================================================
-- PIPELINE METRICS TABLE
-- Tracks pipeline execution performance for evaluation
-- =============================================================================
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              VARCHAR(100) NOT NULL,
    pipeline_name       VARCHAR(200) NOT NULL,
    dag_id              VARCHAR(200),
    task_id             VARCHAR(200),
    status              VARCHAR(50) NOT NULL,       -- running, success, failed, retrying
    records_read        BIGINT DEFAULT 0,
    records_written     BIGINT DEFAULT 0,
    records_failed      BIGINT DEFAULT 0,
    throughput_rps      NUMERIC(12, 2),             -- records per second
    processing_time_ms  BIGINT,
    retry_count         INTEGER DEFAULT 0,
    error_message       TEXT,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    metadata            JSONB
);

CREATE INDEX idx_metrics_run ON pipeline_metrics(run_id);
CREATE INDEX idx_metrics_dag ON pipeline_metrics(dag_id);
CREATE INDEX idx_metrics_status ON pipeline_metrics(status);
CREATE INDEX idx_metrics_started ON pipeline_metrics(started_at);

-- =============================================================================
-- INCREMENTAL LOAD WATERMARKS
-- Tracks the last-loaded position for incremental ingestion
-- =============================================================================
CREATE TABLE IF NOT EXISTS ingestion_watermarks (
    id              BIGSERIAL PRIMARY KEY,
    source_name     VARCHAR(200) UNIQUE NOT NULL,
    last_loaded_at  TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00',
    last_event_id   VARCHAR(64),
    last_offset     BIGINT DEFAULT 0,
    total_loaded    BIGINT DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Initialize watermarks for known sources
INSERT INTO ingestion_watermarks (source_name) VALUES
    ('orders_source'),
    ('user_activity_source'),
    ('sensor_source')
ON CONFLICT (source_name) DO NOTHING;

-- =============================================================================
-- DEAD LETTER QUEUE
-- Stores failed events that couldn't be processed after max retries
-- =============================================================================
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(64),
    source          VARCHAR(100),
    payload         JSONB,
    error_message   TEXT NOT NULL,
    retry_count     INTEGER DEFAULT 0,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    kafka_topic     VARCHAR(200),
    kafka_offset    BIGINT
);

-- =============================================================================
-- ALERT LOG TABLE
-- Stores pipeline alerts and notifications
-- =============================================================================
CREATE TABLE IF NOT EXISTS alert_log (
    id              BIGSERIAL PRIMARY KEY,
    alert_type      VARCHAR(100) NOT NULL,          -- sla_breach, high_failure_rate, lag
    severity        VARCHAR(20) NOT NULL,            -- info, warning, critical
    message         TEXT NOT NULL,
    pipeline_name   VARCHAR(200),
    metric_value    NUMERIC,
    threshold_value NUMERIC,
    resolved        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ
);

-- =============================================================================
-- HELPER FUNCTION: Auto-update updated_at timestamp
-- =============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_orders_updated_at
    BEFORE UPDATE ON processed_orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER set_watermarks_updated_at
    BEFORE UPDATE ON ingestion_watermarks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant all permissions to pipeline_user on all tables
GRANT ALL ON ALL TABLES IN SCHEMA public TO pipeline_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO pipeline_user;
