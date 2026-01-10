-- Simple date dimension derived from stg_fares (demo-friendly)

select distinct
  snapshot_date as date_day,
  extract(dow from snapshot_date) as day_of_week,
  extract(month from snapshot_date) as month,
  extract(year from snapshot_date) as year
from {{ ref('stg_fares') }}
