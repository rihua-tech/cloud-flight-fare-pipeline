-- Redshift COPY template (replace placeholders or use env vars)
-- Required placeholders: {{S3_BUCKET}}, {{S3_PREFIX}}, {{IAM_ROLE_ARN}}, {{REDSHIFT_SCHEMA_RAW}}
-- S3_PREFIX should include the partition path (example: bronze/flights/dt=YYYY-MM-DD)

copy {{REDSHIFT_SCHEMA_RAW}}.fares
from 's3://{{S3_BUCKET}}/{{S3_PREFIX}}/fares.jsonl'
iam_role '{{IAM_ROLE_ARN}}'
format as json 'auto'
timeformat 'auto'
truncatecolumns
blanksasnull
emptyasnull;

-- Parquet alternative (commented)
-- copy {{REDSHIFT_SCHEMA_RAW}}.fares
-- from 's3://{{S3_BUCKET}}/{{S3_PREFIX}}/'
-- iam_role '{{IAM_ROLE_ARN}}'
-- format as parquet;
