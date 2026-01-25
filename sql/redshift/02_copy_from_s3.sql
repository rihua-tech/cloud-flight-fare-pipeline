
copy "raw".fares

from 's3://cloud-flight-fare-pipeline-rihua-2026-east1/bronze/dt=2026-01-23/fares.csv'
iam_role 'arn:aws:iam::183047399603:role/RedshiftCopyRole'
csv
ignoreheader 1
timeformat 'auto'
dateformat 'auto'
region 'us-east-1'
blanksasnull
emptyasnull
acceptinvchars;