# Week 7 Runbook - Real AWS Bronze S3 Ingestion

Week 7 proves Bronze ingestion writes real partitioned CSV snapshots to AWS S3.
It does not claim Redshift, EventBridge, ECS, or CloudWatch productionization.

## Output Layout

Partition files:

```text
s3://<bucket>/bronze/dt=YYYY-MM-DD/fares.csv
```

Run manifest:

```text
s3://<bucket>/bronze/_manifests/bronze_ingestion_<run_timestamp>.json
```

The manifest includes run timestamp, requested dates, processed dates, row counts,
output S3 paths, partition statuses, and rerun behavior.

## Rerun Behavior

Default behavior is `overwrite`.

That means reruns intentionally replace `fares.csv` at the same deterministic
date partition path, for example:

```text
s3://<bucket>/bronze/dt=2026-01-17/fares.csv
```

Use `--rerun-behavior skip-existing` when you want to leave existing partitions
unchanged. That mode checks whether each S3 object already exists before writing.

## Prerequisites

1. Install dependencies:

```powershell
pip install -r requirements.txt
```

2. Configure AWS credentials outside git:

```powershell
aws configure sso
# or: aws configure --profile <profile-name>
```

3. Set environment variables in PowerShell:

```powershell
$env:AWS_REGION="us-east-1"
$env:AWS_PROFILE="<your-profile-name>"
$env:S3_BUCKET="<your-bucket-name>"
$env:S3_PREFIX_BRONZE="bronze"
$env:BRONZE_RERUN_BEHAVIOR="overwrite"
```

Do not put real AWS access keys in `.env.example` or commit them anywhere.

Required AWS permissions for this proof are scoped to the bucket/prefix:
`s3:PutObject` and `s3:HeadObject`. The optional verification commands below
also need `s3:ListBucket` and `s3:GetObject`.

## Run S3 Bronze Ingestion

Run 3 sample dates:

```powershell
python -m ingestion.ingest_api_to_s3 --mode s3 --start 2026-01-17 --days 3
```

Run up to 7 sample dates:

```powershell
python -m ingestion.ingest_api_to_s3 --mode s3 --start 2026-01-17 --days 7
```

If no API credentials are configured, the script writes deterministic synthetic
sample rows. This is acceptable for the Week 7 storage proof because the target
being proved is real AWS S3 Bronze storage.

## Verify in AWS CLI

List partitions:

```powershell
aws s3 ls "s3://$env:S3_BUCKET/bronze/" --recursive
```

Inspect one CSV:

```powershell
aws s3 cp "s3://$env:S3_BUCKET/bronze/dt=2026-01-17/fares.csv" -
```

Inspect the latest manifest:

```powershell
aws s3 ls "s3://$env:S3_BUCKET/bronze/_manifests/"
aws s3 cp "s3://$env:S3_BUCKET/bronze/_manifests/<manifest-file>.json" -
```

## Local Mode Still Works

The default mode remains local and writes to `data/bronze`:

```powershell
python -m ingestion.ingest_api_to_s3 --start 2026-01-17 --days 3
python scripts/load_sample_to_postgres.py
dbt build --project-dir dbt/flight_fares --profiles-dir dbt
```

Local mode also writes a manifest under:

```text
data/bronze/_manifests/
```

## Proof Screenshots

Save Week 7 proof images under:

```text
docs/screenshots/week7/
```

Recommended screenshots:

- Terminal output from the S3 ingestion command showing uploaded partitions and manifest path.
- AWS S3 console showing `bronze/dt=YYYY-MM-DD/` folders.
- AWS S3 console showing `bronze/_manifests/`.
- Manifest JSON preview showing row counts, output S3 paths, and rerun behavior.
- Optional rerun proof showing `overwritten` status or `skipped_existing` status.
