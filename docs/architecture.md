# Architecture

## Local demo
**Postgres + dbt** are used so reviewers can run the project without cloud accounts.

Flow:
1) `scripts/load_sample_to_postgres.py` loads sample data into `raw.fares`
2) dbt builds:
   - `stg_fares` (cleaned)
   - marts: `dim_date`, `dim_route`, `fact_fares`
3) `scripts/run_analysis_queries.py` runs example SQL in `sql/analysis/`

## Production (AWS/Redshift)
Typical flow:
1) **Ingestion (Python):** external API -> S3 `bronze/` partitioned by run date (`dt=YYYY-MM-DD`)
2) **Load (SQL):** Redshift `COPY` from S3 -> `raw.fares` / `staging`
3) **Modeling (dbt):** build star schema marts with tests + docs
4) **Orchestration (Airflow/MWAA):** schedule, retries, backfills, alerting

## Data model (marts)
- `dim_route(origin, dest, route_key)`
- `dim_date(date_day, day_of_week, month, year)`
- `fact_fares(snapshot_date, date_day, origin, dest, depart_date, lead_time_days, price_usd, ...)`
