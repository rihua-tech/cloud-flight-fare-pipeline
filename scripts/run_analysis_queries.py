"""Run a few 'proof' SQL queries against the local marts.

Assumes dbt build has created marts.fact_fares and dims.

Run:
  python scripts/run_analysis_queries.py
"""
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]

def pg_url() -> str:
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE", "fare_db")
    user = os.getenv("PGUSER", "fare_user")
    pwd = os.getenv("PGPASSWORD", "fare_pass")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"



def run_file(conn, path: Path) -> None:
    sql = path.read_text(encoding="utf-8")

    # FIX: dbt created raw_marts.* (and raw_staging.*), but demo SQL uses marts.*
    sql = (
        sql.replace("marts.", "raw_marts.")
           .replace("staging.", "raw_staging.")
    )

    rows = conn.execute(text(sql)).fetchall()
    print(f"\n--- {path.name} ({len(rows)} rows) ---")
    for r in rows[:10]:
        print(tuple(r))


def main() -> None:
    engine = create_engine(pg_url())
    with engine.begin() as conn:
        for q in sorted((ROOT / "sql" / "analysis").glob("*.sql")):
            run_file(conn, q)

if __name__ == "__main__":
    main()
