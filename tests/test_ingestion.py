from ingestion.ingest_api_to_s3 import (
    PartitionResult,
    build_manifest,
    local_path_for_date,
    manifest_key_for_run,
    s3_key_for_date,
    s3_uri_for_date,
    should_skip_partition,
    synthetic_snapshot,
)


def test_synthetic_snapshot_has_rows():
    rows = synthetic_snapshot("2026-01-01")
    assert isinstance(rows, list)
    assert len(rows) == 3
    assert "price_usd" in rows[0]


def test_local_path_format():
    p = local_path_for_date("2026-01-01")
    assert "dt=2026-01-01" in str(p)


def test_s3_path_generation_uses_bronze_partition_layout():
    assert s3_key_for_date("2026-01-01", prefix="bronze") == "bronze/dt=2026-01-01/fares.csv"
    assert (
        s3_uri_for_date("example-bucket", "2026-01-01", prefix="bronze")
        == "s3://example-bucket/bronze/dt=2026-01-01/fares.csv"
    )


def test_manifest_key_generation_uses_bronze_manifest_prefix():
    key = manifest_key_for_run("2026-05-04T15:30:00Z", prefix="bronze")
    assert key == "bronze/_manifests/bronze_ingestion_20260504T153000Z.json"


def test_manifest_generation_includes_required_week7_metadata():
    results = [
        PartitionResult(
            run_date="2026-01-01",
            status="written",
            row_count=3,
            output_path="s3://example-bucket/bronze/dt=2026-01-01/fares.csv",
        ),
        PartitionResult(
            run_date="2026-01-02",
            status="overwritten",
            row_count=3,
            output_path="s3://example-bucket/bronze/dt=2026-01-02/fares.csv",
        ),
    ]

    manifest = build_manifest(
        run_timestamp="2026-05-04T15:30:00Z",
        mode="s3",
        rerun_behavior="overwrite",
        results=results,
        manifest_path="s3://example-bucket/bronze/_manifests/run.json",
    )

    assert manifest["run_timestamp"] == "2026-05-04T15:30:00Z"
    assert manifest["dates_processed"] == ["2026-01-01", "2026-01-02"]
    assert manifest["row_counts_per_date"] == {"2026-01-01": 3, "2026-01-02": 3}
    assert manifest["output_s3_paths"]["2026-01-01"].endswith("/dt=2026-01-01/fares.csv")
    assert manifest["rerun_behavior"] == "overwrite"


def test_skip_existing_rerun_behavior_only_skips_when_partition_exists():
    assert should_skip_partition("skip-existing", partition_exists=True)
    assert not should_skip_partition("skip-existing", partition_exists=False)
    assert not should_skip_partition("overwrite", partition_exists=True)
