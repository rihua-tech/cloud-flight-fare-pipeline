

select count(*) as raw_cnt from bronze.fares;
select *
from raw.fares
limit 10;

select count(*) as staging_cnt from staging.fares;
select *
from 
staging.fares 
limit 20;
