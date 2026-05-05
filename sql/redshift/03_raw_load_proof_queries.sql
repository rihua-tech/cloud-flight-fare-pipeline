-- Week 8 raw load proof queries.
-- Run after sql/redshift/02_copy_from_s3.sql succeeds.

select
  count(*) as raw_row_count
from "{{REDSHIFT_SCHEMA_RAW}}".fares;

select
  min(snapshot_date) as min_snapshot_date,
  max(snapshot_date) as max_snapshot_date,
  count(distinct snapshot_date) as snapshot_date_count
from "{{REDSHIFT_SCHEMA_RAW}}".fares;

select
  snapshot_date,
  count(*) as row_count
from "{{REDSHIFT_SCHEMA_RAW}}".fares
group by 1
order by 1;

select
  sum(case when snapshot_date is null then 1 else 0 end) as snapshot_date_nulls,
  sum(case when origin is null or trim(origin) = '' then 1 else 0 end) as origin_nulls,
  sum(case when dest is null or trim(dest) = '' then 1 else 0 end) as dest_nulls,
  sum(case when depart_date is null then 1 else 0 end) as depart_date_nulls,
  sum(case when price_usd is null then 1 else 0 end) as price_usd_nulls
from "{{REDSHIFT_SCHEMA_RAW}}".fares;

select
  snapshot_date,
  origin,
  dest,
  depart_date,
  price_usd,
  scrape_ts,
  gate,
  trip_class,
  number_of_changes
from "{{REDSHIFT_SCHEMA_RAW}}".fares
order by snapshot_date, origin, dest, depart_date
limit 20;
