# Redshift load (template)

Use:
- `sql/redshift/00_reset_schemas.sql`
- `sql/redshift/01_create_raw_table.sql`
- `sql/redshift/02_copy_from_s3.sql`
- `sql/redshift/verify_marts.sql` (proof row counts after dbt)

In MWAA / job container:
1) Create schemas/tables
2) COPY staging from `s3://<bucket>/<prefix>/dt=YYYY-MM-DD/`
3) Run dbt build (staging + marts + tests)
