-- Postgres DDL for local demo

create schema if not exists raw;
create schema if not exists staging;
create schema if not exists marts;

create table if not exists raw.fares (
  snapshot_date date,
  origin text,
  dest text,
  depart_date date,
  airline text,
  cabin text,
  price_usd numeric(10,2),
  scrape_ts timestamptz
);
