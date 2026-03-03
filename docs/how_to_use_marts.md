# How to Use Marts

## Purpose
This document explains how analysts should query the marts produced by this pipeline for route, timing, and pricing analysis.

## Main marts and what each is for
- `marts.fact_fares`: primary analytics fact table with observed fares and context (`snapshot_date`, `depart_date`, `lead_time_days`, `price_usd`, `origin`, `dest`).
- `marts.dim_route`: route lookup table (`origin`, `dest`, `route_key`) for route-level grouping and labels.
- `marts.dim_date`: snapshot-date calendar dimension (`date_day`, `day_of_week`, `month`, `year`) for date slicing.

Recommended join patterns:
- Join `marts.fact_fares` to `marts.dim_date` on `fact_fares.date_day = dim_date.date_day`.
- Join to `marts.dim_route` on `origin` + `dest` when you need a standardized `route_key`.

## Common business questions
1. Which routes are most expensive on average?
2. What is the minimum observed fare by route?
3. How does price change by booking lead-time bucket?
4. Are weekend departures more expensive than weekday departures?
5. How do route-level average fares change over snapshot dates?

## Where query examples live
- `sql/analysis/`: primary runnable analysis SQL used by `scripts/run_analysis_queries.py`.
- `sql/examples/`: starter templates for analyst onboarding.

## Analyst usage notes
- Grain: `marts.fact_fares` is one observed fare snapshot record; aggregate before visualization.
- Lead-time definition: `lead_time_days = depart_date - snapshot_date`.
- Weekday/weekend rule used in existing queries: `EXTRACT(DOW FROM depart_date) IN (0, 6)` = weekend.
- Data quality: dbt tests enforce non-null checks on core fact columns and uniqueness checks on dimension keys.
