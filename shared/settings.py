"""Centralized settings for the repo."""
# ruff: noqa: I001
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parents[1]
_ROOT_ENV_FILE = _REPO_ROOT / ".env"


class Settings(BaseSettings):
    """
    Centralized configuration for all services.

    Values come from environment variables (and optionally a .env file).
    """

    model_config = SettingsConfigDict(
        # Always load the root .env for local runs, regardless of current working directory.
        env_file=str(_ROOT_ENV_FILE),
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
    )

    # General
    APP_ENV: str = "dev"
    LOG_LEVEL: str = "INFO"

    # Kafka
    KAFKA_TOPIC: str = "sensor-events"
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:29092"
    KAFKA_GROUP_ID: str = "anomaly-detection-consumer"

    # Postgres
    PG_HOST: str = "localhost"
    PG_PORT: int = 5433
    PG_DB: str = "iot_pipeline"
    PG_USER: str = "admin"
    PG_PASSWORD: str = "admin"
    PG_POOL_MIN: int = 1
    PG_POOL_MAX: int = 10

    # API
    API_KEY: str = ""
    API_BASE_URL: str = "http://localhost:8000"

    # Alerts
    SLACK_WEBHOOK_URL: str | None = None
    ALERT_COOLDOWN_SECONDS: float = 1.0

    # MLflow / MinIO (optional)
    USE_MLFLOW_MODEL: bool = False
    MLFLOW_TRACKING_URI: str | None = None
    MLFLOW_MODEL_URI: str | None = None

    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None
    AWS_DEFAULT_REGION: str | None = None
    MLFLOW_S3_ENDPOINT_URL: str | None = None


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

