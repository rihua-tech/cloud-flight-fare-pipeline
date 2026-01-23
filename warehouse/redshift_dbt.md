# Redshift dbt target (example)

Example-only setup for a Redshift warehouse target. Use placeholders and env vars.

## 1) Install dbt-redshift
```bash
pip install dbt-redshift
```

## 2) Create a profile (example)
```bash
cp dbt/profiles.example.yml dbt/profiles.yml
```

Set environment variables (no credentials in git):
```
REDSHIFT_HOST=
REDSHIFT_PORT=5439
REDSHIFT_DBNAME=dev
REDSHIFT_USER=
REDSHIFT_PASSWORD=
REDSHIFT_SCHEMA_RAW=raw

S3_BUCKET=
S3_PREFIX=bronze/flights/dt=YYYY-MM-DD
IAM_ROLE_ARN=
```

## 3) Create schemas/tables
Run the templates in:
- `sql/redshift/create_schemas.sql`
- `sql/redshift/copy_commands.sql` (load `raw.fares`)

Or use the helper:
```bash
python warehouse/run_redshift_sql.py
```

dbt models will land in `staging` and `marts` schemas.

## 4) Run dbt against Redshift
```bash
dbt build --project-dir dbt/flight_fares --profiles-dir dbt --target redshift
```

## 5) Proof row counts (marts)
Run:
- `sql/redshift/verify_marts.sql`
