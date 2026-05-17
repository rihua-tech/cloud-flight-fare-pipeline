# Analytics layer

This folder is reserved for lightweight analytics outputs built from the project’s warehouse-ready tables.

The project’s main reporting tables are produced through dbt models in Redshift Serverless:

- `marts.fact_fares`
- `marts.dim_route`
- `marts.dim_date`

## Current status

The AWS data pipeline proof is complete through:

EventBridge Scheduler → ECS/Fargate → Flight API → S3 Bronze → Redshift Serverless → dbt staging/marts/tests → CloudWatch Logs

The analytics layer is intentionally small for now. The project currently focuses on proving the cloud data engineering pipeline, warehouse loading, dbt transformations, and operational run evidence.

## Suggested analysis queries

Example SQL queries for exploring the mart tables are stored in:

- `sql/analysis/`

These queries can be used to check fare trends, route-level patterns, and reporting-ready outputs from the dbt mart layer.

## Future dashboard work

A BI dashboard can be added later using tools such as Power BI or Tableau.

Recommended dashboard views:

- Average fare by route
- Fare trend over time
- Lowest observed fare by route
- Daily ingestion volume
- Route-level summary metrics

Dashboard screenshots should be added only after a real dashboard is created.
