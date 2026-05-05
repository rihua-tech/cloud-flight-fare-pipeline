select count(*) as raw_cnt from "{{REDSHIFT_SCHEMA_RAW}}".fares;

select *
from "{{REDSHIFT_SCHEMA_RAW}}".fares
limit 10;

select count(*) as staging_cnt from staging.stg_fares;

select *
from staging.stg_fares
limit 20;
