select
  origin,
  dest,
  case
    when extract(dow from depart_date) in (0,6) then 'weekend'
    else 'weekday'
  end as day_type,
  round(avg(price_usd)::numeric, 2) as avg_price_usd,
  count(*) as n
from raw_marts.fact_fares
group by 1,2,3
order by 1,2,3
limit 50;
