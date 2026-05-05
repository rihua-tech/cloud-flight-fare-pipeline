from pathlib import Path

from warehouse.run_redshift_sql import build_s3_copy_uri, render_sql


def test_build_s3_copy_uri_supports_multi_day_bronze_prefix():
    uri = build_s3_copy_uri("example-bucket", "bronze/dt=")

    assert uri == "s3://example-bucket/bronze/dt="


def test_build_s3_copy_uri_keeps_exact_csv_path():
    uri = build_s3_copy_uri("example-bucket", "bronze/dt=2026-01-17/fares.csv")

    assert uri == "s3://example-bucket/bronze/dt=2026-01-17/fares.csv"


def test_build_s3_copy_uri_appends_filename_for_single_date_partition():
    uri = build_s3_copy_uri("example-bucket", "bronze/dt=2026-01-17")

    assert uri == "s3://example-bucket/bronze/dt=2026-01-17/fares.csv"


def test_render_sql_prefers_exact_s3_copy_uri(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("S3_BUCKET", "ignored-bucket")
    monkeypatch.setenv("S3_PREFIX", "ignored-prefix")
    monkeypatch.setenv("S3_COPY_URI", "s3://example-bucket/bronze/dt=")
    monkeypatch.setenv("IAM_ROLE_ARN", "arn:aws:iam::123456789012:role/example")
    monkeypatch.setenv("REDSHIFT_SCHEMA_RAW", "raw")

    sql = (
        'copy "{{REDSHIFT_SCHEMA_RAW}}".fares '
        "from '{{S3_COPY_URI}}' "
        "iam_role '{{IAM_ROLE_ARN}}' "
        "region '{{AWS_REGION}}';"
    )

    rendered = render_sql(sql)

    assert 'copy "raw".fares' in rendered
    assert "from 's3://example-bucket/bronze/dt='" in rendered
    assert "region 'us-west-2'" in rendered


def test_redshift_raw_table_accepts_text_trip_class():
    root = Path(__file__).resolve().parents[1]
    ddl = (root / "sql" / "redshift" / "01_create_raw_table.sql").read_text(encoding="utf-8")

    assert "trip_class varchar(64)" in ddl
