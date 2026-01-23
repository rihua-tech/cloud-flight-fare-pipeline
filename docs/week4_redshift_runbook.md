# Week 4 Redshift Runbook

This runbook shows how to run the pipeline on a real warehouse (Redshift) without committing secrets.

## AWS resources checklist
- Redshift Serverless (workgroup + namespace)
- IAM role for COPY with S3 read access
- Security group that allows your client IP to connect to Redshift

## Required env vars
Set these in your shell or `.env` (do not commit secrets):
```
REDSHIFT_HOST=your-redshift-endpoint
REDSHIFT_PORT=5439
REDSHIFT_DBNAME=dev
REDSHIFT_USER=your_user
REDSHIFT_PASSWORD=your_password
REDSHIFT_SCHEMA_RAW=raw

S3_BUCKET=cloud-flight-fare-pipeline-rihua-2026
S3_PREFIX=bronze/flights/dt=YYYY-MM-DD
IAM_ROLE_ARN=arn:aws:iam::123456789012:role/REDSHIFT_COPY_ROLE
```

## Create a dbt profile
```bash
cp dbt/profiles.example.yml dbt/profiles.yml
```

## Run warehouse SQL (schemas + COPY)
```bash
python warehouse/run_redshift_sql.py
```

## Run dbt against Redshift
```bash
dbt debug --project-dir dbt/flight_fares --profiles-dir dbt --target redshift
dbt build --project-dir dbt/flight_fares --profiles-dir dbt --target redshift
```

## Proof queries (row counts)
Run in Redshift (replace `raw` if you changed the schema):
```
select 'raw.fares' as table_name, count(*) as row_count from raw.fares
union all
select 'staging.stg_fares' as table_name, count(*) as row_count from staging.stg_fares
union all
select 'marts.dim_date' as table_name, count(*) as row_count from marts.dim_date
union all
select 'marts.dim_route' as table_name, count(*) as row_count from marts.dim_route
union all
select 'marts.fact_fares' as table_name, count(*) as row_count from marts.fact_fares;
```
