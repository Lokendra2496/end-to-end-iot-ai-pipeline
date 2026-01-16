from datetime import datetime, timezone
import json
import os

import joblib
import mlflow
from mlflow.models.signature import infer_signature
import mlflow.sklearn
import pandas as pd
import psycopg2
from sklearn.ensemble import IsolationForest

from shared.settings import get_settings

s = get_settings()

PG_HOST = os.getenv("PG_HOST", s.PG_HOST)
PG_PORT = os.getenv("PG_PORT", str(s.PG_PORT))  # your host port is 5433
PG_DB = os.getenv("PG_DB", s.PG_DB)
PG_USER = os.getenv("PG_USER", s.PG_USER)
PG_PASSWORD = os.getenv("PG_PASSWORD", s.PG_PASSWORD)

# MLflow is exposed from docker-compose as host:5001 -> container:5000
mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", s.MLFLOW_TRACKING_URI or "http://127.0.0.1:5001"))
mlflow.set_experiment("iot-anomaly-detection")

# If MLflow is configured to use MinIO/S3 as the artifact store (s3://mlflow-artifacts/),
# the *client* also needs credentials and the endpoint URL.
os.environ.setdefault("AWS_ACCESS_KEY_ID", os.getenv("AWS_ACCESS_KEY_ID", "minio"))
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", os.getenv("AWS_SECRET_ACCESS_KEY", "minio12345"))
os.environ.setdefault("AWS_DEFAULT_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
os.environ.setdefault("MLFLOW_S3_ENDPOINT_URL", os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://127.0.0.1:9100"))

FEATURES = ["temperature", "pressure", "vibration"]
MODEL_NAME = "isolation_forest"

def fetch_training_data(limit: int = 5000) -> pd.DataFrame:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )

    query = f"""
        SELECT temperature, pressure, vibration
        FROM sensor_events
        ORDER BY timestamp ASC
        LIMIT {limit}
    """

    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        raise ValueError("No data found in sensor_events. Run producer+consumer first.")
    return df

def train_model(df: pd.DataFrame) -> IsolationForest:
    model = IsolationForest(
        n_estimators=100,
        contamination=0.005,
        random_state=42,
    )
    model.fit(df)
    return model

def register_model(conn, model_name:str, model_version:str, model_path:str, trained_at, training_rows:int):
    with conn.cursor() as cur:
        # deactivate previous active models for this name
        cur.execute(
            "UPDATE model_registry SET active = False WHERE model_name = %s",
            (model_name,),
        )
        # insert new model row, mark active
        cur.execute(
            """
            INSERT INTO model_registry (model_name, model_version, model_path, trained_at, training_rows, features, active)
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, TRUE)
            """,
            (
                model_name,
                model_version,
                model_path,
                trained_at,
                training_rows,
                json.dumps(FEATURES)
            ),
        )
    conn.commit()

def main():
    with mlflow.start_run():
        
        print("📥 Fetching training data from Postgres...")
        df = fetch_training_data(limit=5000)

        print(f"✅ Training dataset shape: {df.shape}")
        print("🧠 Training Isolation Forest model...")

        model = train_model(df)

        mlflow.log_params({
            "model_name": MODEL_NAME,
            "contamination": 0.005,
            "training_rows": len(df),
        })

        signature = infer_signature(df[FEATURES], model.predict(df[FEATURES]))
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            signature=signature,
            input_example=df[FEATURES].head(3),
            registered_model_name=MODEL_NAME,
        )

        trained_at = datetime.now(timezone.utc)
        model_version = trained_at.strftime("%Y%m%d%H%M%S")

        os.makedirs("models", exist_ok=True)
        model_filename = f"{MODEL_NAME}_{model_version}.joblib"
        model_path = os.path.join("models", model_filename)
        joblib.dump(model, model_path)

        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD
        )
        register_model(conn, MODEL_NAME, model_version, model_path, trained_at,len(df))
        conn.close()

        print(f"✅ Model saved at: {model_path}")

if __name__ == "__main__":
    main()