"""Load sample fare data into local Postgres for a runnable demo.

Creates schemas + raw.fares table (fresh), then loads the newest
`data/bronze/dt=*/fares.csv` if present, falling back to `data/sample/fares_sample.csv`.

Run:
  python scripts/load_sample_to_postgres.py
"""

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import Date, Integer, Numeric, TIMESTAMP, Text as SqlText

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")  # load repo .env reliably


def pg_url() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "fare_db")
    user = os.getenv("PGUSER", "fare_user")
    pwd = os.getenv("PGPASSWORD", "")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


# IMPORTANT: raw table should match the bronze CSV headers
DDL = """
create schema if not exists raw;
create schema if not exists staging;
create schema if not exists marts;

drop table if exists raw.fares cascade;

create table raw.fares (
  snapshot_date date,
  origin varchar(8),
  dest varchar(8),
  depart_date date,
  price_usd numeric(10,2),
  scrape_ts timestamptz,
  gate text,
  trip_class int,
  number_of_changes int
);
"""


def resolve_csv_path() -> Path:
    bronze_dir = ROOT / "data" / "bronze"
    bronze_candidates = sorted(bronze_dir.glob("dt=*/fares.csv"))
    if bronze_candidates:
        return bronze_candidates[-1]

    return ROOT / "data" / "sample" / "fares_sample.csv"


def main() -> None:
    engine = create_engine(pg_url(), future=True)

    csv_path = resolve_csv_path()
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]  # normalize headers

    # Align to expected raw.fares schema
    if "gate" not in df.columns and "airline" in df.columns:
        df["gate"] = df["airline"]

    for missing_col in ["trip_class", "number_of_changes"]:
        if missing_col not in df.columns:
            df[missing_col] = pd.NA

    expected_cols = [
        "snapshot_date",
        "origin",
        "dest",
        "depart_date",
        "price_usd",
        "scrape_ts",
        "gate",
        "trip_class",
        "number_of_changes",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[expected_cols]

    # Parse/clean types (safe even if some values are missing/bad)
    for col in ["snapshot_date", "depart_date", "scrape_ts"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "price_usd" in df.columns:
        df["price_usd"] = pd.to_numeric(df["price_usd"], errors="coerce")

    if "trip_class" in df.columns:
        df["trip_class"] = pd.to_numeric(df["trip_class"], errors="coerce").astype("Int64")

    if "number_of_changes" in df.columns:
        df["number_of_changes"] = pd.to_numeric(df["number_of_changes"], errors="coerce").astype("Int64")

    with engine.begin() as conn:
        conn.execute(text(DDL))

        df.to_sql(
            "fares",
            con=conn,
            schema="raw",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
            dtype={
                "snapshot_date": Date(),
                "origin": SqlText(),
                "dest": SqlText(),
                "depart_date": Date(),
                "price_usd": Numeric(10, 2),
                "scrape_ts": TIMESTAMP(timezone=True),
                "gate": SqlText(),
                "trip_class": Integer(),
                "number_of_changes": Integer(),
            },
        )

    print(f"Loaded {len(df)} rows into raw.fares from {csv_path}")


if __name__ == "__main__":
    main()
