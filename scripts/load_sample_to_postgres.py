"""Load sample fare data into local Postgres for a runnable demo.

Creates schemas + raw.fares table, then loads `data/sample/fares_sample.csv`.

Run:
  python scripts/load_sample_to_postgres.py
"""

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]

def pg_url() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "fare_db")
    user = os.getenv("PGUSER", "fare_user")
    pwd = os.getenv("PGPASSWORD", "")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

DDL = """create schema if not exists raw;
create schema if not exists staging;
create schema if not exists marts;

create table if not exists raw.fares (
  snapshot_date date,
  origin text,
  dest text,
  depart_date date,
  airline text,
  cabin text,
  price_usd numeric(10,2),
  scrape_ts timestamptz
);
"""

def main() -> None:
    engine = create_engine(pg_url())
    csv_path = ROOT / "data" / "sample" / "fares_sample.csv"
    df = pd.read_csv(csv_path, parse_dates=["snapshot_date", "depart_date", "scrape_ts"])

    with engine.begin() as conn:
        conn.execute(text(DDL))
        conn.execute(text("truncate table raw.fares;"))
        df.to_sql("fares", con=conn, schema="raw", if_exists="append", index=False)

    print(f"Loaded {len(df)} rows into raw.fares from {csv_path}")

if __name__ == "__main__":
    main()
