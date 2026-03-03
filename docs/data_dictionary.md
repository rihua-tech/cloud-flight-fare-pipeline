# Data Dictionary

This dictionary documents the core marts in `dbt/flight_fares/models/marts/`.

## marts.fact_fares

| Column Name | Type | Description | Example |
|---|---|---|---|
| snapshot_date | date | Date when fare was observed/scraped. | 2026-01-24 |
| date_day | date | Date key used to join `marts.dim_date`; currently same as `snapshot_date`. | 2026-01-24 |
| origin | text | Origin airport code (uppercased in staging). | ATL |
| dest | text | Destination airport code (uppercased in staging). | JFK |
| depart_date | date | Flight departure date. | 2026-02-12 |
| lead_time_days | integer | Days between departure and snapshot (`depart_date - snapshot_date`). | 19 |
| price_usd | numeric(10,2) | Observed fare in USD. | 289.99 |
| scrape_ts | timestamp (tz) | Source scrape timestamp. | 2026-01-24 15:22:31+00 |
| provider | text | Fare provider or carrier field from source (`airline` or `gate` when present). | Delta |
| trip_class | text | Cabin or trip class from source, when available. | economy |
| number_of_changes | integer | Number of stops/changes from source, when available. | 0 |

## marts.dim_route

| Column Name | Type | Description | Example |
|---|---|---|---|
| origin | text | Origin airport code. | ATL |
| dest | text | Destination airport code. | JFK |
| route_key | text | Route identifier (`origin || '-' || dest`). | ATL-JFK |

## marts.dim_date

| Column Name | Type | Description | Example |
|---|---|---|---|
| date_day | date | Snapshot date key. | 2026-01-24 |
| day_of_week | integer | Day-of-week number extracted from `date_day` (`0` Sunday ... `6` Saturday in Postgres). | 6 |
| month | integer | Month number extracted from `date_day`. | 1 |
| year | integer | Year extracted from `date_day`. | 2026 |

## Assumptions and Notes
- Type labels are logical model types based on dbt SQL; exact physical types can vary slightly by warehouse (Postgres vs Redshift).
- `provider`, `trip_class`, and `number_of_changes` may be `NULL` when those fields are missing in the upstream raw source.
- `stg_fares` filters out rows where `price_usd` is null before marts are built.
