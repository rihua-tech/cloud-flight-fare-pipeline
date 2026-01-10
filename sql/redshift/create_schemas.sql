-- Redshift DDL (template)

create schema if not exists raw;
create schema if not exists staging;
create schema if not exists marts;

-- Example raw table (align with your ingestion schema)
create table if not exists raw.fares (
  snapshot_date date,
  origin varchar(8),
  dest varchar(8),
  depart_date date,
  airline varchar(16),
  cabin varchar(16),
  price_usd decimal(10,2),
  scrape_ts timestamp
);
