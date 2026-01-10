-- Avg price by route and snapshot date (for trend charts)

select
  f.snapshot_date,
  f.origin,
  f.dest,
  round(avg(f.price_usd)::numeric, 2) as avg_price_usd,
  count(*) as samples
from raw_marts.fact_fares f
group by 1,2,3
order by 1,2,3
limit 50;
