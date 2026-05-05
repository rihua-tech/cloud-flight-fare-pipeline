# Redshift load (template)

Use:
- `sql/redshift/00_reset_schemas.sql`
- `sql/redshift/01_create_raw_table.sql`
- `sql/redshift/02_copy_from_s3.sql`
- `sql/redshift/03_raw_load_proof_queries.sql` (raw load proof after COPY)
- `sql/redshift/verify_marts.sql` (proof row counts after dbt)

Week 8 AWS proof path:
1) Create/reset schemas and `raw.fares`.
2) COPY Bronze CSV data from S3 into Redshift Serverless.
3) Run raw proof queries.
4) Run dbt against the `redshift` target.
5) Run mart proof queries.

The helper accepts either an exact `S3_COPY_URI` or `S3_BUCKET` + `S3_PREFIX`.

Examples:
- Single partition: `S3_COPY_URI=s3://<bucket>/bronze/dt=2026-01-17/fares.csv`
- Multi-day prefix: `S3_COPY_URI=s3://<bucket>/bronze/dt=`
- Backward-compatible envs: `S3_BUCKET=<bucket>` and `S3_PREFIX=bronze/dt=`

Run:

```bash
python warehouse/run_redshift_sql.py --dry-run
python warehouse/run_redshift_sql.py
python warehouse/run_redshift_sql.py --files 03_raw_load_proof_queries.sql
```
