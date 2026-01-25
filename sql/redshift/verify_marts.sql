-- Proof queries after dbt build (row counts)
-- Replace {{REDSHIFT_SCHEMA_RAW}} if you use a different raw schema.
select '{{REDSHIFT_SCHEMA_RAW}}.fares' as table_name, count(*) as row_count from {{REDSHIFT_SCHEMA_RAW}}.fares
union all
select 'staging.stg_fares' as table_name, count(*) as row_count from staging.stg_fares
union all
select 'marts.dim_date' as table_name, count(*) as row_count from marts.dim_date
union all
select 'marts.dim_route' as table_name, count(*) as row_count from marts.dim_route
union all
select 'marts.fact_fares' as table_name, count(*) as row_count from marts.fact_fares;
