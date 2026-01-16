# ruff: noqa: E402, I001
from __future__ import annotations

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import json
import random
import time
from datetime import UTC, datetime
import uuid

from faker import Faker
from kafka import KafkaProducer

from shared.logging import configure_logging, get_logger
from shared.settings import get_settings
from shared.retry import retry

fake = Faker()

ANOMALY_PROBABILITY = 0.1
settings = get_settings()
configure_logging(service_name="producer", level=settings.LOG_LEVEL)
log = get_logger(__name__)


def generate_sensor_event(sensor_id: str, anomaly_probability: float = ANOMALY_PROBABILITY) -> dict:
    event = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now(UTC).isoformat(),
        "temperature": round(random.uniform(65, 85), 2),
        "pressure": round(random.uniform(95, 110), 2),
        "vibration": round(random.uniform(0.01, 0.05), 4),
        "event_id": str(uuid.uuid4()),
    }

    # Inject anomalies with the given probability by pushing values far outside normal ranges
    if random.random() < anomaly_probability:
        anomaly_type = random.choice(
            ["temp_spike", "temp_drop", "pressure_spike", "pressure_drop", "vibration_spike"]
        )
        if anomaly_type == "temp_spike":
            event["temperature"] = round(random.uniform(95, 110), 2)
        elif anomaly_type == "temp_drop":
            event["temperature"] = round(random.uniform(35, 50), 2)
        elif anomaly_type == "pressure_spike":
            event["pressure"] = round(random.uniform(130, 160), 2)
        elif anomaly_type == "pressure_drop":
            event["pressure"] = round(random.uniform(60, 80), 2)
        elif anomaly_type == "vibration_spike":
            event["vibration"] = round(random.uniform(0.15, 0.5), 4)

        event["anomaly_injected"] = True
        event["anomaly_type"] = anomaly_type

    return event


def main():
    def _create_producer() -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    producer = retry(_create_producer, name="kafka_producer_connect")

    sensor_ids = [f"sensor_{i}" for i in range(1, 6)]

    log.info(
        "producer_started",
        extra={"kafka_topic": settings.KAFKA_TOPIC, "kafka_bootstrap": settings.KAFKA_BOOTSTRAP_SERVERS},
    )

    try:
        while True:
            sensor_id = random.choice(sensor_ids)
            event = generate_sensor_event(sensor_id, anomaly_probability=ANOMALY_PROBABILITY)

            producer.send(settings.KAFKA_TOPIC, value=event)
            if event.get("anomaly_injected"):
                log.info("sent_anomaly", extra={"event": event})
            else:
                log.info("sent_event", extra={"event": event})

            time.sleep(1)

    except KeyboardInterrupt:
        log.info("producer_stopping_keyboard_interrupt")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()