# Cloud Flight Fare Pipeline
[![CI](https://github.com/rihua-tech/cloud-flight-fare-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/rihua-tech/cloud-flight-fare-pipeline/actions/workflows/ci.yml)

## Contents
- [Architecture Overview](#architecture-overview)
- [Project Overview](#project-overview)
- [Quickstart](#quickstart-local-demo-in-10-minutes)
- [Architecture](#architecture-high-level)
- [Repo Structure](#repo-structure)
- [Local Demo](#local-demo-runs-without-aws)
- [Production](#production-notes-awsredshift)
- [Analytics-ready Output (Week 6)](#analytics-ready-output-week-6)
- [Dashboard Preview](#dashboard-preview-artifact)

## Architecture Overview

![Pipeline Architecture](docs/images/pipeline_architecture.png)

End-to-end data pipeline architecture showing ingestion,
raw storage, transformation with dbt, and analytics outputs.

## Project Overview

This project demonstrates an end-to-end data engineering pipeline for collecting, staging, transforming, and analyzing airline fare data. It supports a fast **local demo environment** (Docker + Postgres) and a **production-style architecture** (AWS S3 + Redshift) orchestrated with Airflow and modeled with dbt.

Outputs include analytics-ready mart tables and example queries that support BI dashboards and pricing analysis workflows.

**AWS • Airflow • Python • SQL • dbt • Redshift (prod) • Postgres (local demo)**

An end-to-end, **Data Engineering** pipeline with an **Analytics + (optional) Data Science** layer.

- **DE core:** ingest → bronze → transform → load → dbt marts → tests/docs → orchestration
- **DA layer:** example SQL queries + ready-to-chart mart tables
- **DS optional:** simple “Buy vs Wait” baseline model trained from mart features
---

## Quickstart (Local Demo in 10 Minutes)

### Prereqs
- Python 3.11+
- Docker Desktop (running)
- dbt (installed in your venv)

### 1) Clone + env
```bash
git clone https://github.com/rihua-tech/cloud-flight-fare-pipeline.git
cd cloud-flight-fare-pipeline
python -m venv .venv
# Windows
.venv\Scripts\activate
pip install -r requirements.txt
```
### 2) Start Postgres (Docker)
```
docker compose up -d
```
### 3) Load sample data into Postgres
```
python scripts/load_sample_to_postgres.py
```
### 4) Run dbt (staging + marts + tests)
```
dbt build --project-dir dbt/flight_fares --profiles-dir dbt
```
### 5) Run analysis queries (proof queries)
```
python scripts/run_analysis_queries.py
```
### Verify tables exist (optional)
```
docker exec -it cloud-flight-fare-pipeline-postgres-1 psql -U fare_user -d fare_db -c "\dt marts.*"
```
###### Note: `dbt/profiles.yml` is ignored (credentials). Use `dbt/profiles.example.yml` as a template and create your own local `dbt/profiles.yml`.

✅ Then run:

```bash
git add README.md
git commit -m "Add Quickstart section"
git push

```

---

## Why this project (business story)
Travel apps and planners struggle with “**When should I book?**” because fares change by route, lead time, seasonality, and volatility.
This pipeline produces clean, tested tables that support:
- route/lead-time trends
- price alerts
- buy/wait recommendations

---

## Architecture (high level)
1) **Ingestion (Python):** API → S3 (bronze) OR local filesystem demo  
2) **Transform (Python or Spark):** bronze → parquet (silver)  
3) **Warehouse load (SQL/Python):** silver → Redshift (prod) or Postgres (local)  
4) **Modeling (dbt):** staging → star schema marts (dim/fact) + tests + docs  
5) **Analytics & DS:** example queries + optional baseline model

See: `docs/architecture.md`

---

## Repo structure
- `ingestion/` – API ingestion (Python) + local demo mode
- `transform/` – bronze → silver transforms (pandas) + optional `spark_jobs/`
- `warehouse/` – loaders + warehouse helpers (Postgres local / Redshift prod templates)
- `sql/` – DDL + COPY templates + analysis queries
- `dbt/flight_fares/` – staging + marts + tests + docs
- `airflow/` – DAG outline (how you’d orchestrate in production)
- `analytics/` – “proof” queries + quick EDA notes
- `ml/` – optional baseline buy/wait model
- `ci/` – GitHub Actions (lint + unit tests + dbt build)

---

## Local demo (runs without AWS)
### 0) Start Postgres
```bash
docker compose up -d postgres
```

### 1) Load sample data into `raw.fares`
```bash
pip install -r requirements.txt
python scripts/load_sample_to_postgres.py
```

### 2) Run dbt build (models + tests)
```bash
cp dbt/profiles.example.yml dbt/profiles.yml
dbt deps --project-dir dbt/flight_fares --profiles-dir dbt
dbt build --project-dir dbt/flight_fares --profiles-dir dbt
```

### 3) Run example analytics queries
```bash
python scripts/run_analysis_queries.py
```

### 4) (Optional) Train baseline model
```bash
python ml/train_buy_wait.py
```

---

## Production notes (AWS/Redshift)
- Redshift DDL/COPY templates: `sql/redshift/`
- Redshift dbt target setup (example only): `warehouse/redshift_dbt.md`
- Proof row counts after dbt: `sql/redshift/verify_marts.sql`
- Airflow DAG outline: `airflow/dags/flight_fare_pipeline_dag.py`
- Replace the local demo loader with:
  - API → S3 ingestion
  - Redshift COPY from S3
  - dbt runs in a job container / MWAA

---


## Week 3 — S3 Bronze Ingestion ✅

This step ingests daily fare snapshots and writes them to S3 in **bronze** partitioned folders.

### Command I ran (3 days)
```bash
python -m ingestion.ingest_api_to_s3 --start 2026-01-17 --days 3 --to-s3
```

### S3 path convention

Current Bronze layout (CSV):
```text
s3://<bucket>/bronze/dt=YYYY-MM-DD/fares.csv
```

*(Legacy note: an earlier version used `bronze/flights/.../fares.jsonl`. If your bucket still has that layout, keep using it — but the Week 4 Redshift COPY helper expects the CSV path above.)*


✅ Real examples (3 days):
- `s3://cloud-flight-fare-pipeline-rihua-2026-east1/bronze/dt=2026-01-22/fares.csv`
- `s3://cloud-flight-fare-pipeline-rihua-2026-east1/bronze/dt=2026-01-23/fares.csv`
- `s3://cloud-flight-fare-pipeline-rihua-2026-east1/bronze/dt=2026-01-24/fares.csv`

### Evidence (screenshots)
 (screenshots)

**Terminal output**
![Terminal upload](docs/screenshots/week3/terminal-upload.png)

**S3 console folders**
![S3 console](docs/screenshots/week3/s3-console.png)


## What recruiters should look at (fast)
- **DE:** `ingestion/`, `warehouse/`, `dbt/`, `sql/redshift/`, `ci/`
- **DA:** `dbt/…/marts/` + `sql/analysis/` + `analytics/`
- **DS:** `ml/` + feature query in `sql/analysis/buy_wait_features.sql`

## Week 4 — Warehouse Target (Summary)

**Option A (Local demo): Postgres “warehouse mode”**
1) docker compose up -d
2) python scripts/load_sample_to_postgres.py (or the psql files if you keep that method)
3) dbt build -t pg_warehouse
4) Link: warehouse/postgres_local.md (or docs/...)

**Option B (AWS): S3 → Redshift Serverless → dbt**
1) python warehouse/run_redshift_sql.py
2) dbt debug -t redshift
3) dbt build -t redshift
4) Link: docs/week4_redshift_runbook.md

## Week 4 — Warehouse target (S3 → Redshift Serverless → dbt) ✅

This repo can run against **Redshift Serverless** as a second “warehouse target” (in addition to local Postgres demo).

**High-level flow**
1) Bronze file exists in S3 (CSV)
2) Run warehouse SQL helper to reset schemas + create `raw.fares` + `COPY` from S3
3) Run dbt against the `redshift` target (staging + marts + tests)
4) Run proof queries in a SQL client (Query Editor v2 / DBeaver)

**Commands (PowerShell)**
```powershell
# (Optional) dry-run prints rendered SQL (no changes in Redshift)
python warehouse/run_redshift_sql.py --dry-run

# Executes: reset schemas + create raw table + COPY from S3
python warehouse/run_redshift_sql.py

# dbt connectivity check
dbt debug --project-dir dbt/flight_fares --profiles-dir dbt -t redshift

# build models + run tests
dbt build --project-dir dbt/flight_fares --profiles-dir dbt -t redshift
```

**Proof queries (run in a SQL client, not PowerShell)**
```sql
select count(*) from raw.fares;
select count(*) from marts.fact_fares;
select * from marts.dim_route limit 10;
```

See `docs/week4_redshift_runbook.md` for full steps + “no secrets committed” notes.


---

## Week 5 — Orchestration (Airflow, Local)

Implemented a local Airflow orchestration demo for the **Cloud Flight Fare Pipeline** to satisfy the Week 5 requirement: **scheduled runs + retries + logs**.

### DAG implemented

**DAG ID:** `flight_fare_pipeline_local_demo`
**Schedule:** `@daily`
**Retries:** `2` (with `retry_delay=3 minutes`)
**Catchup:** `False`

### Task flow (end-to-end)

1. `load_sample_to_postgres` — loads sample fare data into local Postgres
2. `dbt_build` — runs `dbt deps` and `dbt build` (models + tests)
3. `run_analysis_queries` — runs proof/analysis SQL queries

### Run locally (Week 5 demo)

From the repo root:

```bash
# Start project Postgres (warehouse demo)
docker compose up -d

# Start Airflow services (webserver + scheduler + init)
docker compose -f airflow/docker-compose.airflow.yml up -d --build
```

Open Airflow UI:

* `http://localhost:8080`

### Logs and rerun behavior proof

* Task logs are captured in the **Airflow UI** (Task → Logs)
* Rerun behavior was validated using **Clear and Retry** on a successful task (`run_analysis_queries`) in Graph view

### Week 5 screenshots

Saved under:

* `docs/screenshots/week5/`

Included proof:

* DAG list page
* Graph view (3 tasks connected)
* Task log output (`dbt_build`) with success
* Rerun proof (`Clear and Retry` dialog + rerun success)

---

## Week 6 — Documentation & Analytics Handoff

Portfolio handoff for reviewers: this section summarizes how to run and validate the pipeline.
Detailed implementation evidence remains in Weeks 3-5 above.

### Architecture

Detailed architecture: [docs/architecture.md](docs/architecture.md).

End-to-end flow:
1. Ingest fare snapshots to S3 bronze (or local demo input)
2. Transform raw records into curated warehouse-ready data
3. Load warehouse tables (local Postgres or AWS Redshift)
4. Build dbt marts for analytics consumption
5. Serve analytics queries and dashboard outputs

Core marts in this repo:
- `marts.fact_fares`
- `marts.dim_route`
- `marts.dim_date`

### Running Locally

Use this fast local path:

```bash
docker compose up -d
python scripts/load_sample_to_postgres.py
dbt build --project-dir dbt/flight_fares --profiles-dir dbt
python scripts/run_analysis_queries.py
```

For full setup context, see **Quickstart** and **Local demo (runs without AWS)** above.

### Running on AWS

AWS execution path (S3 + Redshift + dbt):

```bash
python warehouse/run_redshift_sql.py --dry-run
python warehouse/run_redshift_sql.py
dbt build --project-dir dbt/flight_fares --profiles-dir dbt -t redshift
```

For full production steps, see **Production notes (AWS/Redshift)** and `docs/week4_redshift_runbook.md`.

### Testing & CI

CI is automated with **GitHub Actions** in `.github/workflows/ci.yml` on pushes to `main` and pull requests.

Pipeline checks include:
- `ruff check .`
- `pytest -q`
- demo data load: `python scripts/load_sample_to_postgres.py`
- dbt validation: `dbt deps` + `dbt build`

### Example Queries & Outputs

Representative analytics queries run on the mart layer:

```sql
select
  origin || '-' || dest as route,
  avg(price_usd) as avg_price_usd
from marts.fact_fares
group by 1
order by avg_price_usd asc
limit 10;
```

```sql
select
  d.year,
  d.month,
  avg(f.price_usd) as monthly_avg_price_usd
from marts.fact_fares f
join marts.dim_date d on f.date_day = d.date_day
group by 1, 2
order by 1, 2;
```

Expected output categories:
- route-level fare trends
- monthly fare movement
- lead-time and weekday/weekend comparisons

Reference query sets:
- `sql/examples/`
- `sql/analysis/`

Sample materialized outputs:
- `analytics/outputs/`

### Analytics-ready Output (Week 6)

Week 6 deliverable is an analytics-ready mart layer for BI, ad-hoc SQL, and downstream modeling.

Primary output tables:
- `marts.fact_fares` (observed fare records with lead-time context)
- `marts.dim_route` (route dimension: origin, destination, route key)
- `marts.dim_date` (calendar dimension by snapshot date)

Supporting documentation:
- `docs/how_to_use_marts.md`
- `docs/data_dictionary.md`
- `docs/kpi_definitions.md`

### Dashboard Preview Artifact
- [docs/images/dashboard_screenshot.png](docs/images/dashboard_screenshot.png)

![Dashboard screenshot](docs/images/dashboard_screenshot.png)

---

