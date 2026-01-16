from datetime import datetime
from typing import List, Optional
import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))
from fastapi import Depends, FastAPI, HTTPException, Header, Query
from psycopg2.extras import RealDictCursor

from api.db import close_pool, get_db, init_pool
from api.schemas import Anomaly, SensorEvent

from shared.logging import configure_logging, get_logger
from shared.settings import get_settings


settings = get_settings()
configure_logging(service_name="api", level=settings.LOG_LEVEL)
log = get_logger(__name__)


def require_api_key(x_api_key: str = Header(default="")):
    if not settings.API_KEY:
        return
    if x_api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


app = FastAPI(title="IOT Anomaly Detection API")

@app.on_event("startup")
def startup_event():
    log.info("starting")
    init_pool()

@app.on_event("shutdown")
def shutdown_event():
    log.info("shutting_down")
    close_pool()

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/events/latest", response_model=List[SensorEvent], dependencies=[Depends(require_api_key)])
def latest_events(sensor_id: Optional[str] = None, limit: int = 20, offset: int = Query(0, ge=0)):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT event_id, sensor_id, timestamp, temperature, pressure, vibration
            FROM sensor_events
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT %s OFFSET %s;
        """

        where_clause = ""
        params = [limit, offset]

        if sensor_id:
            where_clause = "WHERE sensor_id = %s"
            params = [sensor_id, limit, offset]

        
        cur.execute(query.format(where_clause=where_clause), params)
        rows = cur.fetchall()

        cur.close()

        return rows

@app.get("/anomalies/latest", response_model=List[Anomaly], dependencies=[Depends(require_api_key)])
def latest_anomalies(sensor_id: Optional[str] = None, limit: int = 20, offset: int = Query(0, ge=0)):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT event_id, sensor_id, timestamp, temperature, pressure, vibration, anomaly_score, model_name, created_at
            FROM anomalies
            {where_clause}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s;
        """
        
        where_clause = ""
        params = [limit, offset]

        if sensor_id:
            where_clause = "WHERE sensor_id = %s"
            params = [sensor_id, limit]

        cur.execute(query.format(where_clause=where_clause), params)
        rows = cur.fetchall()

        cur.close()
        return rows

@app.get("/anomalies", response_model=List[Anomaly], dependencies=[Depends(require_api_key)])
def query_anomalies(
    sensor_id: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = 100,
    offset: int = Query(0, ge=0),
):
    with get_db() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        base_query = """
            SELECT event_id, sensor_id, timestamp, temperature, pressure, vibration, anomaly_score, model_name, created_at
            FROM anomalies
            WHERE 1=1
        """

        params = []

        if sensor_id:
            base_query += " AND sensor_id = %s"
            params.append(sensor_id)

        if start_time:
            base_query += " AND created_at >= %s"
            params.append(start_time)

        if end_time:
            base_query += " AND created_at <= %s"
            params.append(end_time)
            
        base_query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params.append(limit)
        params.append(offset)

        cur.execute(base_query, params)
        rows = cur.fetchall()

        cur.close()
        return rows