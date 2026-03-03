# KPI Definitions

This document defines the core pricing KPIs and maps each KPI to marts in this repo.

## KPI 1: Average Fare (`avg_fare_usd`)
- Definition: mean fare price in USD for a selected grouping (route, date, lead-time bucket, and so on).
- SQL expression: `AVG(f.price_usd)`
- Business question: What is the typical fare for a route or segment?
- Marts mapping: `marts.fact_fares` (primary), with optional slicing by `marts.dim_date` and `marts.dim_route`.

## KPI 2: Minimum Fare (`min_fare_usd`)
- Definition: lowest observed fare in a selected group.
- SQL expression: `MIN(f.price_usd)`
- Business question: What is the best observed deal for this route and period?
- Marts mapping: `marts.fact_fares`.

## KPI 3: Observation Count (`fare_observation_count`)
- Definition: number of fare observations supporting each aggregated result.
- SQL expression: `COUNT(*)`
- Business question: Is there enough sample volume to trust the metric?
- Marts mapping: `marts.fact_fares`.

## KPI 4: Lead-Time Bucket Average Fare (`avg_fare_by_lead_time_bucket`)
- Definition: average fare grouped by booking-window bucket derived from `lead_time_days`.
- Bucket logic used in repo analysis SQL: `0-6`, `7-13`, `14-29`, `30+`.
- SQL expression: `AVG(f.price_usd)` grouped by a `CASE` expression on `lead_time_days`.
- Business question: How does booking earlier or later impact expected price?
- Marts mapping: `marts.fact_fares`.

## KPI 5: Weekday vs Weekend Average Fare (`avg_fare_weekday_vs_weekend`)
- Definition: average fare split by departure day type.
- Day-type rule used in repo analysis SQL: weekend when `EXTRACT(DOW FROM depart_date) IN (0, 6)`, otherwise weekday.
- SQL expression: `AVG(f.price_usd)` grouped by derived `day_type`.
- Business question: Are weekend departures more expensive than weekday departures?
- Marts mapping: `marts.fact_fares` (uses `depart_date`), with optional snapshot-date slicing via `marts.dim_date`.

## Query References
- `sql/analysis/min_avg_by_route.sql`
- `sql/analysis/lead_time_buckets.sql`
- `sql/analysis/weekday_vs_weekend.sql`
- `sql/analysis/route_price_trends.sql`
