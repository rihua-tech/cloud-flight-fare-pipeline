-- Redshift DDL (template)
-- Replace placeholders or use the helper script: warehouse/run_redshift_sql.py

create schema if not exists {{REDSHIFT_SCHEMA_RAW}};
create schema if not exists staging;
create schema if not exists marts;

-- Example raw table (align with your ingestion schema)
create table if not exists {{REDSHIFT_SCHEMA_RAW}}.fares (
  snapshot_date date,
  origin varchar(8),
  dest varchar(8),
  depart_date date,
  airline varchar(16),
  cabin varchar(16),
  price_usd decimal(10,2),
  scrape_ts timestamp
);
