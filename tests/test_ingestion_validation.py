from ingestion.ingest_api_to_s3 import synthetic_snapshot
from transform.contract import REQUIRED_COLUMNS


def _is_positive_number(value) -> bool:
    try:
        return float(value) > 0
    except (TypeError, ValueError):
        return False


def test_ingestion_snapshot_contains_required_columns():
    rows = synthetic_snapshot("2026-01-01")

    assert rows, "synthetic_snapshot should return at least one row"
    for row in rows:
        assert set(REQUIRED_COLUMNS).issubset(row), f"Missing required columns in row: {row}"
        assert row["snapshot_date"] == "2026-01-01"


def test_ingestion_snapshot_has_positive_numeric_prices():
    rows = synthetic_snapshot("2026-01-01")

    assert rows, "synthetic_snapshot should return at least one row"
    assert all(_is_positive_number(row.get("price_usd")) for row in rows)
