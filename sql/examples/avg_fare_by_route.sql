SELECT
  origin,
  dest,
  ROUND(AVG(price_usd)::numeric, 2) AS avg_price_usd
FROM marts.fact_fares
GROUP BY 1, 2
ORDER BY avg_price_usd DESC;
