{% set fares_source = source('raw', 'fares') %}
{% set source_cols = [] %}
{% if execute %}
  {% for col in adapter.get_columns_in_relation(fares_source) %}
    {% do source_cols.append(col.name | lower) %}
  {% endfor %}
{% endif %}

with source as (
  select * from {{ fares_source }}
)
select
  cast(snapshot_date as date) as snapshot_date,
  upper(origin) as origin,
  upper(dest) as dest,
  cast(depart_date as date) as depart_date,
  cast(price_usd as numeric(10,2)) as price_usd,
  cast(scrape_ts as timestamp) as scrape_ts,
  {% if 'airline' in source_cols %}
    airline
  {% elif 'gate' in source_cols %}
    gate
  {% else %}
    null::text
  {% endif %} as provider,
  {% if 'cabin' in source_cols %}
    cabin
  {% elif 'trip_class' in source_cols %}
    cast(trip_class as text)
  {% else %}
    null::text
  {% endif %} as trip_class,
  {% if 'number_of_changes' in source_cols %}
    cast(number_of_changes as integer)
  {% else %}
    null::integer
  {% endif %} as number_of_changes
from source
where price_usd is not null
