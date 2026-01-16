# ruff: noqa: E402, I001
from __future__ import annotations

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import os
import time

import requests

from shared.logging import get_logger
from shared.settings import get_settings

_last_alert_time = {}
log = get_logger(__name__)
def send_slack_alert(event: dict, score: float):
    s = get_settings()
    webhook_url = s.SLACK_WEBHOOK_URL or os.getenv("SLACK_WEBHOOK_URL")
    try:
        cooldown_seconds = float(s.ALERT_COOLDOWN_SECONDS)
    except ValueError:
        cooldown_seconds = 1.0

    if not webhook_url:
        log.info("slack_webhook_not_set_skipping")
        return
    
    sensor_id = event.get("sensor_id")
    if sensor_id is None:
        log.info("missing_sensor_id_skipping_slack_alert")
        return
    sensor_id = str(sensor_id)
    now = time.time()

    last_time = float(_last_alert_time.get(sensor_id, 0.0))
    delta = now - last_time
    if delta < cooldown_seconds:
        log.info(
            "alert_cooldown_active_skipping",
            extra={"sensor_id": sensor_id, "delta_s": round(delta, 4), "cooldown_s": cooldown_seconds},
        )
        return
    
    anomaly_type = event.get("anomaly_type", "unknown")

    message = {
        "text": "🚨 Anomaly Detected!",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"🚨 *Anomaly Detected*\n"
                        f"*Sensor:* `{event['sensor_id']}`\n"
                        f"*Type:* `{anomaly_type}`\n"
                        f"*Score:* `{score:.4f}`\n\n"
                        f"*Temp:* `{event['temperature']}`\n"
                        f"*Pressure:* `{event['pressure']}`\n"
                        f"*Vibration:* `{event['vibration']}`\n\n"
                        f"*Time:* `{event['timestamp']}`\n"
                        f"*Event ID:* `{event.get('event_id')}`"
                    )
                }
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=message, timeout=5)

        if response.status_code != 200:
            log.info(
                "slack_alert_failed",
                extra={"status_code": response.status_code, "response_text": response.text},
            )
        else:
            # Start cooldown only after a successful send
            _last_alert_time[sensor_id] = now
            log.info("slack_alert_sent", extra={"sensor_id": sensor_id})

    except Exception as e:
        log.info("slack_alert_exception", extra={"error": str(e)})