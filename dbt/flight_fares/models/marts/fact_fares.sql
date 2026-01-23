with fares as (
  select
    snapshot_date,
    origin,
    dest,
    depart_date,
    price_usd,
    scrape_ts,
    provider,
    trip_class,
    number_of_changes,
    (depart_date - snapshot_date) as lead_time_days
  from {{ ref('stg_fares') }}
)

select
  snapshot_date,
  snapshot_date as date_day,
  origin,
  dest,
  depart_date,
  lead_time_days,
  price_usd,
  scrape_ts,
  provider,
  trip_class,
  number_of_changes
from fares

