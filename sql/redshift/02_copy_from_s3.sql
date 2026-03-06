copy "raw".fares
from 's3://{{S3_BUCKET}}/{{S3_PREFIX}}/fares.csv'
iam_role '{{IAM_ROLE_ARN}}'
csv
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
region 'us-east-1'
blanksasnull
emptyasnull
acceptinvchars;
