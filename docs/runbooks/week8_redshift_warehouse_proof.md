# Week 8 Runbook - Real AWS Warehouse Proof

Week 8 proves that real S3 Bronze CSV data can load into Redshift Serverless and
that dbt can build staging and mart models/tests against the Redshift target.

This runbook does not cover EventBridge, ECS, Fargate, or CloudWatch. Those are
not part of Week 8.

## Proof Scope

Source:

```text
s3://<bucket>/bronze/dt=YYYY-MM-DD/fares.csv
```

Warehouse target:

```text
Redshift Serverless -> raw.fares -> staging.stg_fares -> marts.*
```

Primary proof artifacts:

- `raw.fares` row count and date range.
- Raw sample rows and key-column null checks.
- `dbt debug -t redshift` success.
- `dbt build -t redshift` success with tests.
- Mart row counts and sample mart query results.

Save screenshots/logs under:

```text
docs/screenshots/week8/
```

## Required AWS Resources

- S3 bucket containing Week 7 Bronze CSV partitions.
- Redshift Serverless namespace and workgroup.
- Redshift database, usually `dev`.
- Redshift database user with privileges to create schemas/tables.
- IAM role trusted by Redshift and attached to the Serverless namespace or workgroup.
- IAM role policy allowing `s3:GetObject` on the Bronze prefix and `s3:ListBucket`
  on the bucket.
- Network access from your SQL client or local machine to the Redshift Serverless
  endpoint.

Do not commit Redshift passwords, AWS keys, or account-specific private values.

## Redshift Serverless Setup Checklist

1. Create a Redshift Serverless namespace and workgroup in the same AWS Region as
   the S3 bucket, or ensure the COPY command uses the correct `AWS_REGION`.
2. Create or identify a database user that can create schemas and tables.
3. Create an IAM role for COPY with a trust relationship for Redshift.
4. Attach a policy that can read the Bronze prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::<bucket>"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::<bucket>/bronze/*"
    }
  ]
}
```

5. Attach the role to Redshift Serverless as an associated IAM role.

## Local Setup

Install dependencies:

```powershell
pip install -r requirements.txt
```

Copy the dbt profile template locally:

```powershell
Copy-Item dbt\profiles.example.yml dbt\profiles.yml
```

Do not commit `dbt/profiles.yml`.

## Environment Variables

PowerShell example:

```powershell
$env:AWS_REGION="us-east-1"

$env:REDSHIFT_HOST="<workgroup>.<region>.redshift-serverless.amazonaws.com"
$env:REDSHIFT_PORT="5439"
$env:REDSHIFT_DBNAME="dev"
$env:REDSHIFT_USER="<redshift-user>"
$env:REDSHIFT_PASSWORD="<redshift-password>"
$env:REDSHIFT_SCHEMA_RAW="raw"

$env:IAM_ROLE_ARN="arn:aws:iam::<account-id>:role/<redshift-s3-copy-role>"

# Multi-day Week 7 Bronze load. This prefix matches dt=YYYY-MM-DD/fares.csv partitions.
$env:S3_COPY_URI="s3://<bucket>/bronze/dt="
```

Alternative compatibility mode:

```powershell
$env:S3_BUCKET="<bucket>"
$env:S3_PREFIX="bronze/dt="
```

If `S3_COPY_URI` is set, it overrides `S3_BUCKET` and `S3_PREFIX`.

## Dry-Run the SQL

Render the setup and COPY SQL without connecting:

```powershell
python warehouse/run_redshift_sql.py --dry-run
```

Check that the rendered COPY points to the intended S3 source, for example:

```text
s3://<bucket>/bronze/dt=
```

## Load S3 Bronze into Redshift

This command resets the proof schemas, creates `raw.fares`, and runs COPY:

```powershell
python warehouse/run_redshift_sql.py
```

The default SQL files are:

- `sql/redshift/00_reset_schemas.sql`
- `sql/redshift/01_create_raw_table.sql`
- `sql/redshift/02_copy_from_s3.sql`

Warning: `00_reset_schemas.sql` drops and recreates `raw`, `staging`,
`analytics`, and `marts`. Use it only for this proof environment.

## Run Raw Proof Queries

Run in Query Editor v2, DBeaver, or through the helper:

```powershell
python warehouse/run_redshift_sql.py --files 03_raw_load_proof_queries.sql
```

Proof SQL file:

```text
sql/redshift/03_raw_load_proof_queries.sql
```

It checks:

- row count
- min/max snapshot date
- row count by snapshot date
- null checks for key columns
- sample rows

## Run dbt Against Redshift

Connectivity check:

```powershell
dbt debug --project-dir dbt/flight_fares --profiles-dir dbt -t redshift
```

Build staging, marts, and tests:

```powershell
dbt build --project-dir dbt/flight_fares --profiles-dir dbt -t redshift
```

Expected dbt schemas:

- `staging`
- `marts`

## Run Mart Proof Queries

Run:

```sql
select count(*) from staging.stg_fares;
select count(*) from marts.fact_fares;
select count(*) from marts.dim_route;
select count(*) from marts.dim_date;
select * from marts.fact_fares limit 20;
```

Or use:

```text
sql/redshift/verify_marts.sql
```

## Screenshots to Capture

Save proof screenshots/logs under `docs/screenshots/week8/`:

- Redshift Serverless namespace/workgroup with attached IAM role.
- `python warehouse/run_redshift_sql.py --dry-run` rendered COPY source.
- Successful `python warehouse/run_redshift_sql.py` execution.
- Raw proof query output: row count, min/max date, and null checks.
- `dbt debug -t redshift` success.
- `dbt build -t redshift` success with tests.
- Mart proof query output from `staging` or `marts`.

## Success Checklist

- [ ] Bronze CSV exists in S3 from Week 7.
- [ ] Redshift Serverless can assume the S3 COPY IAM role.
- [ ] `raw.fares` is populated by COPY.
- [ ] Raw proof queries return expected rows and dates.
- [ ] `dbt debug -t redshift` succeeds.
- [ ] `dbt build -t redshift` succeeds.
- [ ] Mart proof queries return rows.
- [ ] Proof screenshots/logs are saved in `docs/screenshots/week8/`.
