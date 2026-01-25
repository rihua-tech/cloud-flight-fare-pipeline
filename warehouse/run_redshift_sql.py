"""
Run Redshift warehouse SQL helpers using env var credentials.

Examples:
  python warehouse/run_redshift_sql.py --help
  python warehouse/run_redshift_sql.py --dry-run
  python warehouse/run_redshift_sql.py
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_FILES = [
    ROOT / "sql" / "redshift" / "00_reset_schemas.sql",
    ROOT / "sql" / "redshift" / "01_create_raw_table.sql",
    ROOT / "sql" / "redshift" / "02_copy_from_s3.sql",
]


def get_env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if value is None or value == "":
        return None
    return value


def required_connection_env() -> dict[str, str]:
    missing = []
    host = get_env("REDSHIFT_HOST")
    user = get_env("REDSHIFT_USER")
    password = get_env("REDSHIFT_PASSWORD")
    port = get_env("REDSHIFT_PORT", "5439")
    dbname = get_env("REDSHIFT_DBNAME", "dev")

    if not host:
        missing.append("REDSHIFT_HOST")
    if not user:
        missing.append("REDSHIFT_USER")
    if not password:
        missing.append("REDSHIFT_PASSWORD")

    if missing:
        print("ERROR: Missing required env vars: " + ", ".join(missing))
        print("Tip (PowerShell):")
        print('  $env:REDSHIFT_HOST="example.abc123.us-east-1.redshift.amazonaws.com"')
        print('  $env:REDSHIFT_USER="your_user"')
        print('  $env:REDSHIFT_PASSWORD="your_password"')
        return {}

    return {
        "host": host,
        "user": user,
        "password": password,
        "port": port or "5439",
        "dbname": dbname or "dev",
    }


def render_sql(sql: str) -> str:
    replacements = {
        "S3_BUCKET": get_env("S3_BUCKET"),
        "S3_PREFIX": get_env("S3_PREFIX"),
        "IAM_ROLE_ARN": get_env("IAM_ROLE_ARN"),
        "REDSHIFT_SCHEMA_RAW": get_env("REDSHIFT_SCHEMA_RAW", "raw"),
    }

    placeholders = set(re.findall(r"\{\{([A-Z0-9_]+)\}\}", sql))
    missing = [p for p in placeholders if not replacements.get(p)]
    if missing:
        print("ERROR: Missing required env vars for placeholders: " + ", ".join(missing))
        return ""

    for key, value in replacements.items():
        if value:
            sql = sql.replace(f"{{{{{key}}}}}", value)
    return sql


def execute_sql(conn, sql: str) -> None:
    statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
    with conn.cursor() as cur:
        for stmt in statements:
            cur.execute(stmt)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Run Redshift helper SQL (schemas + COPY commands) using env vars.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print rendered SQL to screen but do not connect/execute.",
    )
    p.add_argument(
        "--files",
        nargs="*",
        help=(
            "Optional list of SQL filenames to run (default runs 00_reset_schemas.sql + "
            "01_create_raw_table.sql + 02_copy_from_s3.sql)."
        ),
    )
    return p


def main() -> int:
    args = build_arg_parser().parse_args()

    files = SQL_FILES
    if args.files:
        files = [ROOT / "sql" / "redshift" / f for f in args.files]

    # Render SQL first (so --dry-run works without psycopg2)
    rendered = []
    for path in files:
        if not path.exists():
            print(f"ERROR: SQL file not found: {path}")
            return 1
        sql = path.read_text(encoding="utf-8")
        sql = render_sql(sql)
        if not sql:
            return 1
        rendered.append((path, sql))

    if args.dry_run:
        for path, sql in rendered:
            print(f"\n--- {path} ---\n{sql}\n")
        print("DRY RUN complete.")
        return 0

    # Only require psycopg2 + connection env vars when executing
    try:
        import psycopg2  # type: ignore
    except Exception:
        print("ERROR: psycopg2 is not installed. Run: pip install psycopg2-binary")
        return 1

    conn_info = required_connection_env()
    if not conn_info:
        return 1

    had_failure = False
    conn = psycopg2.connect(**conn_info)

    with conn:
        for path, sql in rendered:
            try:
                execute_sql(conn, sql)
                print(f"SUCCESS: {path}")
            except Exception as exc:
                print(f"FAILED: {path} -> {exc}")
                had_failure = True

    conn.close()
    return 1 if had_failure else 0


if __name__ == "__main__":
    raise SystemExit(main())
