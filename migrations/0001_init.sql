-- Initial schema for IoT anomaly detection pipeline.
-- This file is intentionally idempotent (CREATE IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS sensor_events (
    event_id TEXT PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    pressure DOUBLE PRECISION NOT NULL,
    vibration DOUBLE PRECISION NOT NULL,
    raw_event JSONB
);

CREATE INDEX IF NOT EXISTS idx_sensor_events_timestamp ON sensor_events (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_sensor_events_sensor_id ON sensor_events (sensor_id);

CREATE TABLE IF NOT EXISTS anomalies (
    event_id TEXT PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    pressure DOUBLE PRECISION NOT NULL,
    vibration DOUBLE PRECISION NOT NULL,
    anomaly_score DOUBLE PRECISION NOT NULL,
    model_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_created_at ON anomalies (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_sensor_id ON anomalies (sensor_id);

CREATE TABLE IF NOT EXISTS model_registry (
    id BIGSERIAL PRIMARY KEY,
    model_name TEXT NOT NULL,
    model_version TEXT NOT NULL,
    model_path TEXT NOT NULL,
    trained_at TIMESTAMPTZ NOT NULL,
    training_rows INTEGER NOT NULL,
    features JSONB NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_model_registry_name ON model_registry (model_name);
CREATE UNIQUE INDEX IF NOT EXISTS ux_model_registry_name_version ON model_registry (model_name, model_version);

