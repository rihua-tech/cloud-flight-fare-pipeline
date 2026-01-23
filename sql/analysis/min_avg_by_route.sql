select
  origin,
  dest,
  round(min(price_usd)::numeric, 2) as min_price_usd,
  round(avg(price_usd)::numeric, 2) as avg_price_usd,
  round(max(price_usd)::numeric, 2) as max_price_usd,
  count(*) as n
from marts.fact_fares
group by 1,2
order by avg_price_usd desc
limit 50;
