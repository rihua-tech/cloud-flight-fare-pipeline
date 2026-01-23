"""Baseline Buy/Wait model (optional).

Simple logistic regression to demonstrate:
features (SQL) -> train -> evaluate.

Run after local demo:
  python ml/train_buy_wait.py
"""
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.linear_model import LogisticRegression

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]

def pg_url() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "fare_db")
    user = os.getenv("PGUSER", "fare_user")
    pwd = os.getenv("PGPASSWORD", "")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

def main() -> None:
    engine = create_engine(pg_url())
    qpath = ROOT / "sql" / "analysis" / "buy_wait_features.sql"
    features_query = qpath.read_text(encoding="utf-8")

    df = pd.read_sql(text(features_query), engine)
    if df.empty:
        raise SystemExit("No features returned. Run dbt build first.")

    X = df[["lead_time_days", "delta_from_3d_min", "price_usd"]].fillna(0)
    y = df["label_buy"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )
    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)

    print(classification_report(y_test, preds, digits=3))

if __name__ == "__main__":
    main()
