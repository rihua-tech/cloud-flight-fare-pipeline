drop table if exists raw.fares;

create table raw.fares (
  snapshot_date date,
  origin varchar(8),
  dest varchar(8),
  depart_date date,
  price_usd decimal(10,2),
  scrape_ts timestamp,
  gate varchar(256),
  trip_class int,
  number_of_changes int
);
