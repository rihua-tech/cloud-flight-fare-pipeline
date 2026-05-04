import argparse
import csv
import io
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

import requests
from dotenv import load_dotenv

from ingestion.config import settings

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
IngestionMode = Literal["local", "s3"]
RerunBehavior = Literal["overwrite", "skip-existing"]
CANONICAL_COLUMNS = [
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


def _prefixed_path(prefix: str, suffix: str) -> str:
    return f"{prefix}/{suffix}" if prefix else suffix


@dataclass(frozen=True)
class PartitionResult:
    run_date: str
    status: str
    row_count: int | None
    output_path: str


def utc_now_iso_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def s3_key_for_date(run_date: str, prefix: str | None = None) -> str:
    prefix = (settings.s3_prefix_bronze if prefix is None else prefix).strip("/")
    return _prefixed_path(prefix, f"dt={run_date}/fares.csv")


def s3_uri_for_date(bucket: str, run_date: str, prefix: str | None = None) -> str:
    return f"s3://{bucket}/{s3_key_for_date(run_date, prefix)}"


def local_path_for_date(run_date: str) -> Path:
    return ROOT / "data" / "bronze" / f"dt={run_date}" / "fares.csv"


def run_id_from_timestamp(run_timestamp: str) -> str:
    return (
        run_timestamp.replace("-", "")
        .replace(":", "")
        .replace(".", "")
        .replace("+0000", "Z")
    )


def manifest_key_for_run(run_timestamp: str, prefix: str | None = None) -> str:
    prefix = (settings.s3_prefix_bronze if prefix is None else prefix).strip("/")
    manifest_name = f"_manifests/bronze_ingestion_{run_id_from_timestamp(run_timestamp)}.json"
    return _prefixed_path(prefix, manifest_name)


def local_manifest_path_for_run(run_timestamp: str) -> Path:
    return ROOT / "data" / "bronze" / "_manifests" / (
        f"bronze_ingestion_{run_id_from_timestamp(run_timestamp)}.json"
    )


def synthetic_snapshot(run_date: str) -> list[dict[str, Any]]:
    rows = [
        {
            "snapshot_date": run_date,
            "origin": "ATL",
            "dest": "JFK",
            "depart_date": "2026-02-14",
            "price_usd": 215.0,
            "gate": "DL",
            "trip_class": "ECON",
            "number_of_changes": 0,
        },
        {
            "snapshot_date": run_date,
            "origin": "ATL",
            "dest": "LAX",
            "depart_date": "2026-02-20",
            "price_usd": 328.0,
            "gate": "DL",
            "trip_class": "ECON",
            "number_of_changes": 0,
        },
        {
            "snapshot_date": run_date,
            "origin": "SFO",
            "dest": "JFK",
            "depart_date": "2026-03-05",
            "price_usd": 299.0,
            "gate": "UA",
            "trip_class": "ECON",
            "number_of_changes": 0,
        },
    ]
    ts = utc_now_iso_z()
    for r in rows:
        r["scrape_ts"] = ts
    return rows


def fetch_snapshot(run_date: str) -> list[dict[str, Any]]:
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


def canonicalize_records(records: list[dict[str, Any]], run_date: str) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    fallback_ts = utc_now_iso_z()
    for row in records:
        snapshot_date = str(row.get("snapshot_date") or run_date)
        origin = str(row.get("origin") or "").upper().strip()
        dest = str(row.get("dest") or "").upper().strip()
        depart_date = str(row.get("depart_date") or "")[:10]
        price_usd = row.get("price_usd", row.get("value"))

        if not origin or not dest or not depart_date or price_usd in (None, ""):
            continue

        normalized.append(
            {
                "snapshot_date": snapshot_date,
                "origin": origin,
                "dest": dest,
                "depart_date": depart_date,
                "price_usd": price_usd,
                "scrape_ts": row.get("scrape_ts") or fallback_ts,
                "gate": row.get("gate") or row.get("airline") or "",
                "trip_class": row.get("trip_class") or row.get("cabin") or "",
                "number_of_changes": row.get("number_of_changes")
                if row.get("number_of_changes") is not None
                else row.get("changes", row.get("stops", "")),
            }
        )
    return normalized


def write_csv_local(records: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CANONICAL_COLUMNS)
        writer.writeheader()
        writer.writerows(records)


def upload_csv_to_s3(records: list[dict[str, Any]], key: str, s3_client: Any) -> None:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CANONICAL_COLUMNS)
    writer.writeheader()
    writer.writerows(records)
    body = buf.getvalue().encode("utf-8")
    s3_client.put_object(Bucket=settings.s3_bucket, Key=key, Body=body)


def s3_object_exists(s3_client: Any, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=settings.s3_bucket, Key=key)
    except Exception as exc:
        error = getattr(exc, "response", {}).get("Error", {})
        if error.get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise
    return True


def should_skip_partition(rerun_behavior: RerunBehavior, partition_exists: bool) -> bool:
    return rerun_behavior == "skip-existing" and partition_exists


def rerun_behavior_note(rerun_behavior: RerunBehavior) -> str:
    if rerun_behavior == "overwrite":
        return (
            "overwrite: reruns intentionally replace fares.csv at the same deterministic "
            "date partition path."
        )
    return "skip-existing: reruns leave existing date partitions unchanged."


def build_manifest(
    *,
    run_timestamp: str,
    mode: IngestionMode,
    rerun_behavior: RerunBehavior,
    results: list[PartitionResult],
    manifest_path: str,
) -> dict[str, Any]:
    output_paths = {result.run_date: result.output_path for result in results}
    row_counts = {
        result.run_date: result.row_count
        for result in results
        if result.row_count is not None
    }

    return {
        "schema_version": 1,
        "run_timestamp": run_timestamp,
        "mode": mode,
        "rerun_behavior": rerun_behavior,
        "rerun_note": rerun_behavior_note(rerun_behavior),
        "requested_dates": [result.run_date for result in results],
        "dates_processed": [
            result.run_date for result in results if result.status != "skipped_existing"
        ],
        "row_counts_per_date": row_counts,
        "output_paths": output_paths,
        "output_s3_paths": output_paths if mode == "s3" else {},
        "manifest_path": manifest_path,
        "partitions": [asdict(result) for result in results],
    }


def write_manifest_local(manifest: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def upload_manifest_to_s3(manifest: dict[str, Any], key: str, s3_client: Any) -> None:
    s3_client.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


def daterange(start_yyyy_mm_dd: str, days: int) -> list[str]:
    start = date.fromisoformat(start_yyyy_mm_dd)
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Write canonical Bronze fare snapshots locally or to real AWS S3 "
            "as dt=YYYY-MM-DD/fares.csv partitions."
        )
    )
    parser.add_argument("--date", default=None, help="Single run date YYYY-MM-DD (optional)")
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD (for multi-day run)")
    parser.add_argument(
        "--days",
        type=int,
        default=1,
        help="Number of days to run; use 3-7 for the Week 7 S3 proof",
    )
    parser.add_argument(
        "--mode",
        choices=("local", "s3"),
        default="local",
        help="Output target. Week 7 real AWS proof uses --mode s3.",
    )
    parser.add_argument(
        "--to-s3",
        action="store_true",
        help="Backward-compatible alias for --mode s3",
    )
    parser.add_argument(
        "--rerun-behavior",
        choices=("overwrite", "skip-existing"),
        default=settings.bronze_rerun_behavior,
        help=(
            "overwrite replaces existing date partitions; skip-existing leaves existing "
            "partitions unchanged"
        ),
    )
    args = parser.parse_args()

    if args.days < 1:
        parser.error("--days must be at least 1")
    if args.rerun_behavior not in ("overwrite", "skip-existing"):
        parser.error("BRONZE_RERUN_BEHAVIOR must be overwrite or skip-existing")

    mode: IngestionMode = "s3" if args.to_s3 else args.mode
    rerun_behavior: RerunBehavior = args.rerun_behavior

    # Decide which dates to run
    if args.start:
        run_dates = daterange(args.start, args.days)
    elif args.date:
        run_dates = [args.date]
    else:
        run_dates = [str(date.today())]

    run_timestamp = utc_now_iso_z()
    s3_client = None
    if mode == "s3":
        if not settings.s3_bucket:
            parser.error("S3_BUCKET must be set for --mode s3")

        import boto3  # optional dependency for real AWS runs

        s3_client = boto3.client("s3", region_name=settings.aws_region)

    total = 0
    results: list[PartitionResult] = []
    for run_date in run_dates:
        if mode == "s3":
            assert s3_client is not None
            key = s3_key_for_date(run_date)
            output_path = s3_uri_for_date(settings.s3_bucket, run_date)
            exists = s3_object_exists(s3_client, key)

            if should_skip_partition(rerun_behavior, exists):
                results.append(
                    PartitionResult(
                        run_date=run_date,
                        status="skipped_existing",
                        row_count=None,
                        output_path=output_path,
                    )
                )
                print(f"Skipped existing partition {output_path}")
                continue

            records = canonicalize_records(fetch_snapshot(run_date), run_date)
            upload_csv_to_s3(records, key, s3_client)
            status = "overwritten" if exists else "written"
            results.append(
                PartitionResult(
                    run_date=run_date,
                    status=status,
                    row_count=len(records),
                    output_path=output_path,
                )
            )
            print(f"Uploaded {len(records)} records to {output_path} ({status})")
        else:
            path = local_path_for_date(run_date)
            output_path = str(path)
            exists = path.exists()

            if should_skip_partition(rerun_behavior, exists):
                results.append(
                    PartitionResult(
                        run_date=run_date,
                        status="skipped_existing",
                        row_count=None,
                        output_path=output_path,
                    )
                )
                print(f"Skipped existing partition {path}")
                continue

            records = canonicalize_records(fetch_snapshot(run_date), run_date)
            write_csv_local(records, path)
            status = "overwritten" if exists else "written"
            results.append(
                PartitionResult(
                    run_date=run_date,
                    status=status,
                    row_count=len(records),
                    output_path=output_path,
                )
            )
            print(f"Wrote {len(records)} records to {path} ({status})")

        total += results[-1].row_count or 0

    if mode == "s3":
        assert s3_client is not None
        manifest_key = manifest_key_for_run(run_timestamp)
        manifest_path = f"s3://{settings.s3_bucket}/{manifest_key}"
        manifest = build_manifest(
            run_timestamp=run_timestamp,
            mode=mode,
            rerun_behavior=rerun_behavior,
            results=results,
            manifest_path=manifest_path,
        )
        upload_manifest_to_s3(manifest, manifest_key, s3_client)
    else:
        local_manifest_path = local_manifest_path_for_run(run_timestamp)
        manifest_path = str(local_manifest_path)
        manifest = build_manifest(
            run_timestamp=run_timestamp,
            mode=mode,
            rerun_behavior=rerun_behavior,
            results=results,
            manifest_path=manifest_path,
        )
        write_manifest_local(manifest, local_manifest_path)

    print(f"Wrote manifest to {manifest_path}")
    print(
        f"Done. mode={mode} days_requested={len(run_dates)} "
        f"dates_processed={len(manifest['dates_processed'])} total_records={total}"
    )


if __name__ == "__main__":
    main()
