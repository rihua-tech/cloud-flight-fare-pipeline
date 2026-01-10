# Cloud Flight Fare Pipeline
**AWS • Airflow • Python • SQL • dbt • Redshift (prod) • Postgres (local demo)**

An end-to-end, **Data Engineering** pipeline with an **Analytics + (optional) Data Science** layer.

- **DE core:** ingest → bronze → transform → load → dbt marts → tests/docs → orchestration
- **DA layer:** example SQL queries + ready-to-chart mart tables
- **DS optional:** simple “Buy vs Wait” baseline model trained from mart features

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
cp dbt/flight_fares/profiles.yml.example ~/.dbt/profiles.yml
cd dbt/flight_fares
dbt deps
dbt build
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
- Airflow DAG outline: `airflow/dags/flight_fare_pipeline_dag.py`
- Replace the local demo loader with:
  - API → S3 ingestion
  - Redshift COPY from S3
  - dbt runs in a job container / MWAA

---

## What recruiters should look at (fast)
- **DE:** `ingestion/`, `warehouse/`, `dbt/`, `sql/redshift/`, `ci/`
- **DA:** `dbt/…/marts/` + `sql/analysis/` + `analytics/`
- **DS:** `ml/` + feature query in `sql/analysis/buy_wait_features.sql`
