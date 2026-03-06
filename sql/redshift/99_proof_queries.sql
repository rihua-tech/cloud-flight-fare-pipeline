select count(*) as raw_cnt from raw.fares;
select *
from raw.fares
limit 10;

select count(*) as staging_cnt from staging.stg_fares;
select *
from 
staging.stg_fares 
limit 20;
