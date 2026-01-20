"""
Ingest flight-fare snapshots and write to S3 (bronze) or local demo folder.

Modes:
1) Local demo (default): writes JSONL to `data/bronze/dt=YYYY-MM-DD/fares.jsonl`
2) S3 (optional): uploads JSONL to `s3://S3_BUCKET/S3_PREFIX_BRONZE/dt=YYYY-MM-DD/fares.jsonl`

If API credentials are not provided, a small synthetic dataset is generated.

Run examples:
  python -m ingestion.ingest_api_to_s3 --date 2026-01-01
  python -m ingestion.ingest_api_to_s3 --date 2026-01-01 --to-s3

Step 6 (multiple days):
  python -m ingestion.ingest_api_to_s3 --start 2026-01-17 --days 3
  python -m ingestion.ingest_api_to_s3 --start 2026-01-17 --days 3 --to-s3
"""

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

from ingestion.config import settings

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]


def s3_key_for_date(run_date: str) -> str:
    prefix = settings.s3_prefix_bronze.strip("/")
    return f"{prefix}/dt={run_date}/fares.jsonl"


def local_path_for_date(run_date: str) -> Path:
    return ROOT / "data" / "bronze" / f"dt={run_date}" / "fares.jsonl"


def synthetic_snapshot(run_date: str) -> List[Dict[str, Any]]:
    rows = [
        {"snapshot_date": run_date, "origin": "ATL", "dest": "JFK", "depart_date": "2026-02-14", "airline": "DL", "cabin": "ECON", "price_usd": 215.0},
        {"snapshot_date": run_date, "origin": "ATL", "dest": "LAX", "depart_date": "2026-02-20", "airline": "DL", "cabin": "ECON", "price_usd": 328.0},
        {"snapshot_date": run_date, "origin": "SFO", "dest": "JFK", "depart_date": "2026-03-05", "airline": "UA", "cabin": "ECON", "price_usd": 299.0},
    ]
    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    for r in rows:
        r["scrape_ts"] = ts
    return rows


def fetch_snapshot(run_date: str) -> List[Dict[str, Any]]:
    if not settings.api_base_url or not settings.api_key:
        return synthetic_snapshot(run_date)

    url = settings.api_base_url.rstrip("/") + "/fares"
    headers = {"Authorization": f"Bearer {settings.api_key}"}
    resp = requests.get(url, params={"date": run_date}, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "results" in data:
        return data["results"]
    if isinstance(data, list):
        return data
    raise ValueError("Unexpected API response format")


def write_jsonl_local(records: List[Dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def upload_jsonl_to_s3(records: List[Dict[str, Any]], key: str) -> None:
    import boto3  # optional import

    if not settings.s3_bucket:
        raise ValueError("S3_BUCKET is not set")

    s3 = boto3.client("s3", region_name=settings.aws_region)
    body = "".join(json.dumps(r) + "\n" for r in records).encode("utf-8")
    s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=body)


def daterange(start_yyyy_mm_dd: str, days: int) -> List[str]:
    start = date.fromisoformat(start_yyyy_mm_dd)
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="Single run date YYYY-MM-DD (optional)")
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD (for multi-day run)")
    parser.add_argument("--days", type=int, default=1, help="Number of days to run (default 1)")
    parser.add_argument("--to-s3", action="store_true", help="Upload to S3 instead of local disk")
    args = parser.parse_args()

    # Decide which dates to run
    if args.start:
        run_dates = daterange(args.start, args.days)
    elif args.date:
        run_dates = [args.date]
    else:
        run_dates = [str(date.today())]

    total = 0
    for run_date in run_dates:
        records = fetch_snapshot(run_date)

        if args.to_s3:
            key = s3_key_for_date(run_date)
            upload_jsonl_to_s3(records, key)
            print(f"Uploaded {len(records)} records to s3://{settings.s3_bucket}/{key}")
        else:
            path = local_path_for_date(run_date)
            write_jsonl_local(records, path)
            print(f"Wrote {len(records)} records to {path}")

        total += len(records)

    print(f"Done. Days={len(run_dates)} total_records={total}")


if __name__ == "__main__":
    main()
