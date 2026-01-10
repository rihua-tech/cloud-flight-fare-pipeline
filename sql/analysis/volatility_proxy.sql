select
  origin,
  dest,
  round(stddev_samp(price_usd)::numeric, 2) as price_stddev_usd,
  round(avg(price_usd)::numeric, 2) as avg_price_usd,
  count(*) as n
from raw_marts.fact_fares
group by 1,2
order by price_stddev_usd desc nulls last
limit 50;
