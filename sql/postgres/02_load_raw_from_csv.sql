\set ON_ERROR_STOP on

-- Guard: fail fast if filepath is missing
\if :{?filepath}
\echo Loading :'filepath'
copy raw.fares from :'filepath' with (format csv, header true);
\else
\echo ERROR: filepath is NOT set.
\echo Run like:
\echo   psql -U fare_user -d fare_db -v filepath=/data/bronze/dt=YYYY-MM-DD/fares.csv -f /sql/postgres/02_load_raw_from_csv.sql
\quit 1
\endif
