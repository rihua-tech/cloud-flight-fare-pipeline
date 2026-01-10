with source as (
  select * from {{ source('raw', 'fares') }}
)
select
  cast(snapshot_date as date) as snapshot_date,
  upper(origin) as origin,
  upper(dest) as dest,
  cast(depart_date as date) as depart_date,
  airline,
  cabin,
  cast(price_usd as numeric(10,2)) as price_usd,
  cast(scrape_ts as timestamp) as scrape_ts
from source
where price_usd is not null
