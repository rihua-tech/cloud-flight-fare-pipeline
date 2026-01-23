copy raw.fares
from 's3://YOUR_BUCKET/bronze/dt=YYYY-MM-DD/fares.csv'
iam_role 'YOUR_REDSHIFT_COPY_ROLE_ARN'
csv
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
blanksasnull
emptyasnull
acceptinvchars;
