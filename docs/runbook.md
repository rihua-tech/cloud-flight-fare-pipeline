# Runbook

## Local demo
```bash
docker compose up -d postgres
pip install -r requirements.txt
python scripts/load_sample_to_postgres.py
cp dbt/flight_fares/profiles.yml.example ~/.dbt/profiles.yml
cd dbt/flight_fares && dbt deps && dbt build
python scripts/run_analysis_queries.py
```

## Common issues
- **dbt can't connect:** check Postgres is running and `.env` / env vars match docker compose
- **port 5432 already used:** stop your local Postgres or change the port mapping in docker-compose
