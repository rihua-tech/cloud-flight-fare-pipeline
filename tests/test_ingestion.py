from ingestion.ingest_api_to_s3 import synthetic_snapshot, local_path_for_date

def test_synthetic_snapshot_has_rows():
    rows = synthetic_snapshot("2026-01-01")
    assert isinstance(rows, list)
    assert len(rows) == 3
    assert "price_usd" in rows[0]

def test_local_path_format():
    p = local_path_for_date("2026-01-01")
    assert "dt=2026-01-01" in str(p)
