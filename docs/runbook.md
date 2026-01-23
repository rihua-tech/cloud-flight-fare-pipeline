# Runbook

## Local demo
```bash
docker compose up -d postgres
pip install -r requirements.txt
python scripts/load_sample_to_postgres.py
cp dbt/profiles.example.yml dbt/profiles.yml
dbt deps --project-dir dbt/flight_fares --profiles-dir dbt
dbt build --project-dir dbt/flight_fares --profiles-dir dbt
python scripts/run_analysis_queries.py
```

## Common issues
- **dbt can't connect:** check Postgres is running and `.env` / env vars match docker compose
- **port 5432 already used:** stop your local Postgres or change the port mapping in docker-compose


## Bronze → Silver (Week 2)

1) Place raw/bronze CSVs into `data/bronze/`

2) Transform to silver parquet:
   - `python -m transform.bronze_to_silver --input data/bronze --output data/silver/flight_fares.parquet`

3) Validate silver:
   - `python -m transform.validate_silver --path data/silver/flight_fares.parquet --report analytics/outputs/validation_report.json`

4) Run unit tests:
   - `pytest -q`

Silver output: `data/silver/flight_fares.parquet`  
Validation report: `analytics/outputs/validation_report.json`

