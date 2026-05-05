drop schema if exists "{{REDSHIFT_SCHEMA_RAW}}" cascade;
drop schema if exists staging cascade;
drop schema if exists analytics cascade;
drop schema if exists marts cascade;

create schema "{{REDSHIFT_SCHEMA_RAW}}";
create schema staging;
create schema analytics;
create schema marts;


