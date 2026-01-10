-- Redshift COPY templates (fill your IAM role + S3 path)

-- 1) COPY JSONL (one JSON object per line)
-- copy raw.fares
-- from 's3://<bucket>/bronze/flights/dt=2026-01-01/fares.jsonl'
-- iam_role 'arn:aws:iam::<account>:role/<RedshiftCopyRole>'
-- format as json 'auto'
-- timeformat 'auto'
-- truncatecolumns
-- blanksasnull
-- emptyasnull;

-- 2) COPY Parquet (recommended for performance)
-- copy raw.fares
-- from 's3://<bucket>/silver/flights/dt=2026-01-01/'
-- iam_role 'arn:aws:iam::<account>:role/<RedshiftCopyRole>'
-- format as parquet;
