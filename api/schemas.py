from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class SensorEvent(BaseModel):
    event_id: Optional[str]
    sensor_id: str
    timestamp: datetime
    temperature: float
    pressure: float
    vibration: float


class Anomaly(BaseModel):
    event_id: Optional[str]
    sensor_id: str
    timestamp: datetime
    temperature: float
    pressure: float
    vibration: float
    anomaly_score: float
    model_name: str
    created_at: datetime