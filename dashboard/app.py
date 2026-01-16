# ruff: noqa: E402, I001
import sys
from pathlib import Path
from datetime import UTC

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

import os
from datetime import datetime, timedelta

import requests
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from shared.settings import get_settings

s = get_settings()
API_KEY = s.API_KEY or os.getenv("API_KEY", "")
API_BASE = s.API_BASE_URL or os.getenv("API_BASE_URL", "http://localhost:8000")

def api_get(path: str, params: dict):
    headers = {"X-API-Key": API_KEY} if API_KEY else {}
    r = requests.get(f"{API_BASE}{path}", params=params, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

st.set_page_config(page_title="IoT Anomaly Dashboard", layout="wide")
st.title("IoT Anomaly Detection Dashboard")

# Sidebar filters
sensor_id = st.sidebar.selectbox("Sensor", ["(all)"] + [f"sensor_{i}" for i in range(1, 6)])
limit = st.sidebar.slider("Rows", 10, 200, 50)

minutes = st.sidebar.slider("Lookback (minutes)", 5, 240, 60)
end_time = datetime.now(UTC)
start_time = end_time - timedelta(minutes=minutes)

params = {
    "limit": limit,
    "start_time": start_time.isoformat().replace("+00:00", "Z"),
    "end_time": end_time.isoformat().replace("+00:00", "Z"),
}
if sensor_id != "(all)":
    params["sensor_id"] = sensor_id

col1, col2 = st.columns(2)

with col1:
    st.subheader("Latest Anomalies")
    anomalies = api_get("/anomalies", params=params)
    df_a = pd.DataFrame(anomalies)
    if df_a.empty:
        st.info("No anomalies in selected window.")
    else:
        st.dataframe(df_a, use_container_width=True)

with col2:
    st.subheader("Latest Events")
    ev_params = {"limit": limit}
    if sensor_id != "(all)":
        ev_params["sensor_id"] = sensor_id
    events = api_get("/events/latest", params=ev_params)
    df_e = pd.DataFrame(events)
    if df_e.empty:
        st.info("No events returned.")
    else:
        st.dataframe(df_e, use_container_width=True)

st.subheader("Anomaly Trend")
if not df_a.empty:
    # Plot anomalies count per minute
    df_a["created_at"] = pd.to_datetime(df_a["created_at"], utc=True)
    df_a["minute"] = df_a["created_at"].dt.floor("min")
    counts = df_a.groupby("minute").size().reset_index(name="count")

    fig = plt.figure()
    plt.plot(counts["minute"], counts["count"])
    plt.xticks(rotation=45)
    st.pyplot(fig, clear_figure=True)