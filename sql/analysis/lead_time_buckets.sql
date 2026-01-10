-- Lead time buckets (days between snapshot and depart_date)

select
  case
    when lead_time_days < 7 then '0-6'
    when lead_time_days < 14 then '7-13'
    when lead_time_days < 30 then '14-29'
    else '30+'
  end as lead_time_bucket,
  origin,
  dest,
  round(avg(price_usd)::numeric, 2) as avg_price_usd,
  count(*) as samples
from (
  select
    snapshot_date,
    origin,
    dest,
    depart_date,
    price_usd,
    (depart_date - snapshot_date) as lead_time_days
  from marts.fact_fares
) t
group by 1,2,3
order by 1,2,3;
