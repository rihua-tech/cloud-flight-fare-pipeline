# Week 4 Redshift Runbook (Serverless)

This runbook covers Week 4 Part B: load Bronze CSV from S3 into **Redshift Serverless** (raw layer) and then build **dbt** staging + marts in this repo (`cloud-flight-fare-pipeline`).

> ⚠️ WARNING: `sql/redshift/00_reset_schemas.sql` DROPS `raw`, `staging`, and `marts` schemas (it will delete existing tables/views).

## Prerequisites

- Redshift Serverless workgroup + namespace created.
- Database user/password that can create schemas/tables.
- IAM Role attached to Redshift Serverless that can read your S3 bucket/prefix (COPY).
- Bronze CSV exists in S3, e.g. `s3://YOUR_BUCKET/bronze/dt=YYYY-MM-DD/fares.csv`
- Local:
  - Python 3.10+
  - `pip install psycopg2-binary`
  - `pip install dbt-redshift`
- Network access to the Redshift Serverless endpoint.

## Step 1 - Confirm repo files

These files should exist in the repo:
- `warehouse/run_redshift_sql.py`
- `sql/redshift/00_reset_schemas.sql`
- `sql/redshift/01_create_raw_table.sql`
- `sql/redshift/02_copy_from_s3.sql`
- `dbt/flight_fares/`
- `dbt/profiles.example.yml` (template for local profiles)


## Step 2 - Create a local dbt profile

Copy the example file and keep it local only:
```powershell
Copy-Item dbt\profiles.example.yml dbt\profiles.yml
```

Do not commit `dbt/profiles.yml` to git.

## Step 3 - Verify Bronze data and S3_PREFIX

Make sure your Bronze CSV exists in S3 under the date partition you plan to load:
- `s3://YOUR_BUCKET/bronze/dt=YYYY-MM-DD/fares.csv`

S3_PREFIX is everything after the bucket name, without a leading slash.
- If your file is `s3://YOUR_BUCKET/bronze/dt=YYYY-MM-DD/fares.csv` then:
  - `S3_PREFIX=bronze/dt=YYYY-MM-DD`(no leading slash)

Optional CLI check:
```bash
aws s3 ls s3://YOUR_BUCKET/bronze/dt=YYYY-MM-DD/
```

## Step 4 - Set environment variables (PowerShell)

Required connection variables:
```powershell
$env:REDSHIFT_HOST="your-workgroup.xxxx.us-east-1.redshift-serverless.amazonaws.com"
$env:REDSHIFT_PORT="5439"
$env:REDSHIFT_DBNAME="dev"
$env:REDSHIFT_USER="your_user"
$env:REDSHIFT_PASSWORD="your_password"

# Optional: used by dbt sources (defaults to raw)
$env:REDSHIFT_SCHEMA_RAW="raw"
```

If you use placeholders in the COPY SQL, also set:
```powershell
$env:S3_BUCKET="YOUR_BUCKET"
$env:S3_PREFIX="bronze/dt=YYYY-MM-DD"
$env:IAM_ROLE_ARN="arn:aws:iam::<acct-id>:role/RedshiftCopyRole"
```

## Step 5 - COPY SQL (recommended: placeholders)

- Edit `sql/redshift/02_copy_from_s3.sql` and replace the S3 path and IAM role:
```sql

copy "raw".fares
from 's3://{{S3_BUCKET}}/{{S3_PREFIX}}/fares.csv'
iam_role '{{IAM_ROLE_ARN}}'
csv
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
region 'us-east-1'
blanksasnull
emptyasnull
acceptinvchars;

...
```

- Then set `S3_BUCKET`, `S3_PREFIX`, and `IAM_ROLE_ARN` (see Step 4).

## Step 6 - Run the Redshift SQL helper

Dry-run (renders SQL without executing):
```powershell
python warehouse/run_redshift_sql.py --dry-run
```

Execute (creates schemas, tables, and runs COPY):
```powershell
python warehouse/run_redshift_sql.py
```

## Step 7 - Run dbt against Redshift

Check connectivity:
```powershell
dbt debug --project-dir dbt/flight_fares --profiles-dir dbt -t redshift
```

Build models and tests:
```powershell
dbt build --project-dir dbt/flight_fares --profiles-dir dbt -t redshift
```

## Step 8 - Proof queries (SQL client only)

Use Query Editor v2 or DBeaver (not PowerShell) and run:
```sql
select count(*) from raw.fares;
select count(*) from marts.fact_fares;
select * from marts.dim_route limit 10;
```

## No Secrets (Do NOT Commit)

Do not commit any of the following:
- `.env`
- `dbt/profiles.yml`
- `dbt/.user.yml`
- `dbt/**/target`
- `dbt/**/logs`

Recommended `.gitignore` entries:
```gitignore
.env
dbt/profiles.yml
dbt/.user.yml
dbt/**/target
dbt/**/logs
```

## Troubleshooting

- Password authentication failed
  - Verify `REDSHIFT_USER` and `REDSHIFT_PASSWORD` are correct.
  - PowerShell can keep old env vars; open a new shell and re-export.

- Missing placeholder env vars (S3_BUCKET, S3_PREFIX, IAM_ROLE_ARN)
  - This occurs only if your SQL uses `{{...}}` placeholders.
  - Set the env vars or replace placeholders with concrete values.

- COPY permissions or role access error
  - Ensure the IAM role is attached to the Redshift Serverless namespace/workgroup.
  - The role needs `s3:GetObject` (and likely `s3:ListBucket`) on the bucket/prefix.

- "select is not recognized" in PowerShell
  - `select` is SQL, not a PowerShell command.
  - Run SQL in Query Editor v2, DBeaver, or another SQL client.

## Success checklist

- [ ] `python warehouse/run_redshift_sql.py --dry-run` renders without errors
- [ ] `python warehouse/run_redshift_sql.py` completes successfully
- [ ] `dbt debug` succeeds against the `redshift` target
- [ ] `dbt build` completes with models and tests passing
- [ ] `select count(*) from raw.fares;` returns > 0
- [ ] `select count(*) from marts.fact_fares;` returns > 0
- [ ] `select * from marts.dim_route limit 10;` returns rows
