SELECT
  CASE
    WHEN lead_time_days < 7 THEN '0-6'
    WHEN lead_time_days < 14 THEN '7-13'
    WHEN lead_time_days < 30 THEN '14-29'
    ELSE '30+'
  END AS lead_time_bucket,
  COUNT(*) AS records,
  ROUND(AVG(price_usd)::numeric, 2) AS avg_price_usd
FROM marts.fact_fares
GROUP BY 1
ORDER BY 1;
