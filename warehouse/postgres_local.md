# Postgres local demo

## Start Postgres

```powershell
docker compose up -d
```

## Create schemas + raw table

```powershell
docker exec -i cloud-flight-fare-pipeline-postgres-1 psql -U fare_user -d fare_db -f /sql/postgres/00_schemas.sql
docker exec -i cloud-flight-fare-pipeline-postgres-1 psql -U fare_user -d fare_db -f /sql/postgres/01_create_raw_table.sql
```

## Load newest bronze CSV (no manual date editing)

```powershell
docker exec -i cloud-flight-fare-pipeline-postgres-1 bash -lc 'LATEST=$(ls -1 /data/bronze | grep ''^dt='' | sort | tail -n 1); FILEPATH=/data/bronze/$LATEST/fares.csv; psql -U fare_user -d fare_db -v filepath="$FILEPATH" -f /sql/postgres/02_load_raw_from_csv.sql'
```

## Proof queries (run inside psql)

```sql
select count(*) from raw.fares;
select * from raw.fares limit 10;
```

## Reminder

Do not commit secrets. Keep `.env` local and use `.env.example` for defaults.


## Part A Proof (Postgres local)

![A5 Postgres raw.fares proof](screenshots/week4/week4_partA_postgres_raw_fares_proof_2026-01-22.png)

![A6 dbt debug + build success](../docs/screenshots/week4/week4_A6_dbt_pg_debug_build_success_2026-01-22.png)

