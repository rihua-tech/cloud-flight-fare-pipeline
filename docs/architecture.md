# Architecture

## Overview
This project builds a flight fare data pipeline that supports both a **local demo workflow** (Postgres + dbt) and a **production-oriented cloud design** (AWS/Redshift). The goal is to produce analytics-ready marts for reporting, ad-hoc querying, and downstream modeling.

Core marts in this repo:
- `marts.fact_fares`
- `marts.dim_route`
- `marts.dim_date`

## Local Demo Flow (Postgres + dbt)
The local path is intentionally runnable without AWS credentials.

Execution flow:
1. Start local Postgres:
   - `docker compose up -d postgres`
2. Load sample data into `raw.fares`:
   - `python scripts/load_sample_to_postgres.py`
3. Build dbt staging + marts + tests:
   - `dbt build --project-dir dbt/flight_fares --profiles-dir dbt`
4. Run analysis SQL and write CSV outputs:
   - `python scripts/run_analysis_queries.py`

Data movement (local):
- `data/sample/fares_sample.csv` (or latest `data/bronze/dt=*/fares.csv`)
- `raw.fares` -> `staging` (`stg_fares`) -> `marts.fact_fares`, `marts.dim_route`, `marts.dim_date`
- analytics outputs written to `analytics/outputs/`

## Production Flow (AWS/Redshift + orchestration)
The production-oriented path keeps the same logical model while changing infrastructure.

Typical flow:
1. Ingest API snapshots to S3 bronze partitions:
   - `python -m ingestion.ingest_api_to_s3 --mode s3 --start YYYY-MM-DD --days N`
2. Load bronze files into Redshift raw schema:
   - `python warehouse/run_redshift_sql.py`
3. Build dbt models/tests in Redshift:
   - `dbt build --project-dir dbt/flight_fares --profiles-dir dbt -t redshift`
4. Orchestrate the sequence with Airflow (local DAG in repo; MWAA-compatible pattern):
   - `airflow/dags/flight_fare_pipeline_dag.py`

## Data Model / Marts
| Mart | Grain | Main Use |
|---|---|---|
| `marts.fact_fares` | One observed fare per `snapshot_date`, `origin`, `dest`, `depart_date` (plus provider/class attributes when available) | KPI calculations: avg fare, min fare, lead-time and weekday/weekend analysis |
| `marts.dim_route` | One row per route (`origin`, `dest`) | Route slicing and route labels (`route_key`) |
| `marts.dim_date` | One row per snapshot date (`date_day`) | Calendar slicing (`day_of_week`, `month`, `year`) |

## Architecture Diagram
Reference artifact: `docs/images/architecture_diagram.png`

![Architecture diagram](images/architecture_diagram.png)
