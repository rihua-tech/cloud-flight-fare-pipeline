"""Run analysis SQL queries against the local Postgres database.

Assumes dbt build has created raw_marts.fact_fares and dims.

Run:
  python scripts/run_analysis_queries.py
"""
import os
import csv
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
ANALYSIS_DIR = ROOT / "sql" / "analysis"
OUTPUT_DIR = ROOT / "analytics" / "outputs"

QUERY_FILES = [
    "route_price_trends.sql",
    "lead_time_buckets.sql",
    "min_avg_by_route.sql",
    "volatility_proxy.sql",
    "weekday_vs_weekend.sql",
]

def pg_url() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "fare_db")
    user = os.getenv("PGUSER", "fare_user")
    pwd = os.getenv("PGPASSWORD", "fare_pass")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def run_file(conn, path: Path, output_dir: Path) -> None:
    sql = path.read_text(encoding="utf-8")

    result = conn.execute(text(sql))
    rows = result.fetchall()
    columns = list(result.keys())

    print(f"\n--- {path.name} ({len(rows)} rows) ---")
    for r in rows[:20]:
        print(tuple(r))

    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{path.stem}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"SUCCESS: wrote {output_path}")


def main() -> None:
    engine = create_engine(pg_url())
    with engine.begin() as conn:
        for filename in QUERY_FILES:
            path = ANALYSIS_DIR / filename
            try:
                run_file(conn, path, OUTPUT_DIR)
            except Exception as exc:
                print(f"FAILED: {path.name} -> {exc}")

if __name__ == "__main__":
    main()
