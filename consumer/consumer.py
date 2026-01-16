# ruff: noqa: E402, I001
from __future__ import annotations

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import json
import os

try:
    # Preferred: run as module (python -m consumer.consumer)
    from consumer.alerts import send_slack_alert
    from consumer.db import get_connection, insert_sensor_event, insert_anomaly
except ModuleNotFoundError:
    # Fallback: run from within consumer/ directory
    from alerts import send_slack_alert  # type: ignore
    from db import get_connection, insert_sensor_event, insert_anomaly  # type: ignore

from dateutil.parser import isoparse
import joblib
from kafka import KafkaConsumer
import pandas as pd
from shared.logging import configure_logging, get_logger
from shared.retry import retry
from shared.settings import get_settings

MODEL_PATH = "ml/models/isolation_forest.joblib"
MODEL_NAME = "isolation_forest_v1"

s = get_settings()
configure_logging(service_name="consumer", level=s.LOG_LEVEL)
log = get_logger(__name__)


def score_event(model, event: dict):
    # Match training feature names: temperature, pressure, vibration
    x = pd.DataFrame(
        [[float(event["temperature"]), float(event["pressure"]), float(event["vibration"])]],
        columns=["temperature", "pressure", "vibration"],
    )

    prediction = model.predict(x)[0]
    anomaly_score = model.decision_function(x)[0]

    is_anomaly = prediction == -1
    return is_anomaly, anomaly_score


def main():
    # If using an MLflow model registry URI (models:/...), MLflow will download model artifacts
    # from the configured artifact store (MinIO/S3 in this setup). The client must have creds + endpoint.
    if s.AWS_ACCESS_KEY_ID:
        os.environ.setdefault("AWS_ACCESS_KEY_ID", s.AWS_ACCESS_KEY_ID)
    if s.AWS_SECRET_ACCESS_KEY:
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", s.AWS_SECRET_ACCESS_KEY)
    if s.AWS_DEFAULT_REGION:
        os.environ.setdefault("AWS_DEFAULT_REGION", s.AWS_DEFAULT_REGION)
    if s.MLFLOW_S3_ENDPOINT_URL:
        os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", s.MLFLOW_S3_ENDPOINT_URL)

    if s.USE_MLFLOW_MODEL:
        # Defer MLflow access until runtime (and after env is loaded) to avoid import-time crashes.
        import mlflow
        import mlflow.sklearn  # noqa: F401

        if not s.MLFLOW_TRACKING_URI or not s.MLFLOW_MODEL_URI:
            raise ValueError("USE_MLFLOW_MODEL is true but MLFLOW_TRACKING_URI/MLFLOW_MODEL_URI not set")

        mlflow.set_tracking_uri(s.MLFLOW_TRACKING_URI)

        log.info(
            "loading_model_from_mlflow",
            extra={"mlflow_model_uri": s.MLFLOW_MODEL_URI, "mlflow_tracking_uri": s.MLFLOW_TRACKING_URI},
        )
        model = mlflow.sklearn.load_model(s.MLFLOW_MODEL_URI)
        active_model_name = f"mlflow:{s.MLFLOW_MODEL_URI}"
    else:
        log.info("loading_model_from_path", extra={"model_path": MODEL_PATH})
        model = joblib.load(MODEL_PATH)
        active_model_name = MODEL_NAME

    log.info("model_loaded", extra={"model_name": active_model_name})

    def _create_consumer() -> KafkaConsumer:
        return KafkaConsumer(
            s.KAFKA_TOPIC,
            bootstrap_servers=s.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=s.KAFKA_GROUP_ID,
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )

    consumer = retry(_create_consumer, name="kafka_consumer_connect")

    conn = get_connection()
    log.info(
        "consumer_started",
        extra={"kafka_topic": s.KAFKA_TOPIC, "kafka_bootstrap": s.KAFKA_BOOTSTRAP_SERVERS},
    )

    try:
        for msg in consumer:
            event = msg.value

            # optional: validate timestamp format early (good production habit)
            event["timestamp"] = isoparse(event["timestamp"])

            insert_sensor_event(conn, event)
            
            is_anomaly, anomaly_score = score_event(model, event)
            if is_anomaly:
                insert_anomaly(conn, event, anomaly_score, active_model_name)
                send_slack_alert(event, anomaly_score)
                log.info(
                    "anomaly_detected",
                    extra={
                        "sensor_id": event.get("sensor_id"),
                        "timestamp": str(event.get("timestamp")),
                        "anomaly_score": float(anomaly_score),
                    },
                )
            else:
                log.info(
                    "normal_event",
                    extra={"sensor_id": event.get("sensor_id"), "timestamp": str(event.get("timestamp"))},
                )

    except KeyboardInterrupt:
        log.info("consumer_stopped_keyboard_interrupt")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()