with source as (
  select * from {{ source('raw', 'fares') }}
)

select
  cast(snapshot_date as date)                        as snapshot_date,
  upper(origin)                                      as origin,
  upper(dest)                                        as dest,
  cast(depart_date as date)                          as depart_date,
  cast(price_usd as numeric(10,2))                   as price_usd,
  cast(scrape_ts as timestamptz)                     as scrape_ts,
  gate                                               as provider,
  trip_class                                         as trip_class,
  number_of_changes                                  as number_of_changes
from source
where price_usd is not null

