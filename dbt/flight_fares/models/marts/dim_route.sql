select distinct
  origin,
  dest,
  origin || '-' || dest as route_key
from {{ ref('stg_fares') }}
