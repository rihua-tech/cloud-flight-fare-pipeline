-- Feature table for a baseline Buy/Wait model.
-- Label rule (demo): BUY=1 if price is <= route 3-day rolling min, else 0.

with base as (
  select
    snapshot_date,
    origin,
    dest,
    depart_date,
    price_usd,
    (depart_date - snapshot_date) as lead_time_days
  from marts.fact_fares
),
roll as (
  select
    *,
    min(price_usd) over (
      partition by origin, dest, depart_date
      order by snapshot_date
      rows between 2 preceding and current row
    ) as rolling_3d_min
  from base
)
select
  snapshot_date,
  origin,
  dest,
  depart_date,
  lead_time_days,
  price_usd,
  (price_usd - rolling_3d_min) as delta_from_3d_min,
  case when price_usd <= rolling_3d_min then 1 else 0 end as label_buy
from roll
order by snapshot_date, origin, dest, depart_date;
