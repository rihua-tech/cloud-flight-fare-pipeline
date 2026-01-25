drop table if exists raw.fares cascade;

create table raw.fares (
  snapshot_date date,
  origin varchar(8),
  dest varchar(8),
  depart_date date,
  price_usd numeric(10,2),
  scrape_ts timestamptz,
  gate text,
  trip_class int,
  number_of_changes int
);

