drop table if exists "{{REDSHIFT_SCHEMA_RAW}}".fares;

create table "{{REDSHIFT_SCHEMA_RAW}}".fares (
  snapshot_date date,
  origin varchar(8),
  dest varchar(8),
  depart_date date,
  price_usd decimal(10,2),
  scrape_ts timestamp,
  gate varchar(256),
  trip_class varchar(64),
  number_of_changes int
);

