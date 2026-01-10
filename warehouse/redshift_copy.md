# Redshift load (template)

Use:
- `sql/redshift/create_schemas.sql`
- `sql/redshift/copy_commands.sql`

In MWAA / job container:
1) Create schemas/tables
2) COPY staging from `s3://<bucket>/<prefix>/dt=YYYY-MM-DD/`
3) Run dbt build (staging + marts + tests)
