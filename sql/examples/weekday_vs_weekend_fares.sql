SELECT
  CASE
    WHEN EXTRACT(DOW FROM depart_date) IN (0, 6) THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_type,
  ROUND(AVG(price_usd)::numeric, 2) AS avg_price_usd
FROM marts.fact_fares
GROUP BY 1;
