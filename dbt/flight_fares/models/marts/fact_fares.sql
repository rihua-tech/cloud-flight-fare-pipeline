with fares as (
  select
    snapshot_date,
    origin,
    dest,
    depart_date,
    airline,
    cabin,
    price_usd,
    scrape_ts,
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
  airline,
  cabin,
  price_usd,
  scrape_ts
from fares
