copy "{{REDSHIFT_SCHEMA_RAW}}".fares
from '{{S3_COPY_URI}}'
iam_role '{{IAM_ROLE_ARN}}'
csv
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
region '{{AWS_REGION}}'
blanksasnull
emptyasnull
acceptinvchars;
