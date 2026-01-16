# ruff: noqa: E402, I001
from __future__ import annotations

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import json
from datetime import datetime

import psycopg2
from psycopg2.extras import Json

from shared.logging import get_logger
from shared.settings import get_settings
from shared.retry import retry

log = get_logger(__name__)

def get_connection():
    s = get_settings()

    def _connect():
        return psycopg2.connect(
            host=s.PG_HOST,
            port=str(s.PG_PORT),
            dbname=s.PG_DB,
            user=s.PG_USER,
            password=s.PG_PASSWORD,
        )

    conn = retry(_connect, name="postgres_connect")
    log.info("postgres_connected")
    return conn


def insert_sensor_event(conn, event: dict):
    def _json_default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type not serializable: {type(obj).__name__}")

    query = """
        INSERT INTO sensor_events (event_id, sensor_id, timestamp, temperature, pressure, vibration, raw_event)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                event["event_id"],
                event["sensor_id"],
                event["timestamp"],
                float(event["temperature"]),
                float(event["pressure"]),
                float(event["vibration"]),
                Json(event, dumps=lambda o: json.dumps(o, default=_json_default)),
            ),
        )
    conn.commit()

def insert_anomaly(conn, event: dict, anomaly_score: float, model_name: str):
    query = """
        INSERT INTO anomalies (
            event_id, sensor_id, timestamp, temperature, pressure, vibration, anomaly_score, model_name
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(
            query,
            (
                event["event_id"],
                event["sensor_id"],
                event["timestamp"],
                float(event["temperature"]),
                float(event["pressure"]),
                float(event["vibration"]),
                float(anomaly_score),
                model_name,
            ),
        )
    conn.commit()