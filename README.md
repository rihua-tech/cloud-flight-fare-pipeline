# Cloud Flight Fare Pipeline
[![CI](https://github.com/rihua-tech/cloud-flight-fare-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/rihua-tech/cloud-flight-fare-pipeline/actions/workflows/ci.yml)

## What This Proves

This is a cloud data engineering proof project for a flight fare analytics
pipeline. It does more than show local scripts: the AWS path was run end to end
with real AWS services, proof screenshots, and CloudWatch success logs.

Implemented cloud path:

```text
EventBridge Scheduler -> ECS / Fargate Batch Container -> Flight API Ingestion -> S3 Bronze -> Redshift Serverless -> dbt staging/marts/tests -> CloudWatch Logs
```

Services and tools used: **EventBridge Scheduler, ECS/Fargate, ECR, S3,
Redshift Serverless, Secrets Manager, CloudWatch Logs, Docker, Python, SQL, and
dbt**.

## Current Proven AWS Path

![Current Proven AWS Path](docs/images/current-proven-aws-path.png)

The ECS/Fargate batch container runs the data pipeline:

```text
Flight API -> S3 Bronze -> Redshift Serverless -> dbt staging/marts/tests
```

The scheduled proof wraps that container with EventBridge Scheduler and
CloudWatch Logs:

```text
EventBridge Scheduler -> ECS / Fargate Batch Container -> Flight API Ingestion -> S3 Bronze -> Redshift Serverless -> dbt staging/marts/tests -> CloudWatch Logs
```

## Proof at a Glance

- Manual ECS/Fargate run completed with exit code `0`.
- EventBridge Scheduler triggered ECS/Fargate.
- CloudWatch logs showed `WEEK9_BATCH_SUCCESS`.
- S3 Bronze ingestion, Redshift load, dbt build/tests, and mart verification
  completed.
- Daily EventBridge schedule was disabled after proof to avoid recurring AWS
  cost.

## What Recruiters Should Look At Fast

- **Cloud DE proof:** Current Proven AWS Path, Proof at a Glance, Evidence,
  and Runbooks.
- **Implementation:** `scripts/run_aws_batch_pipeline.py`, `ingestion/`,
  `warehouse/`, `sql/redshift/`, and `dbt/flight_fares/`.
- **AWS templates:** `aws/*.template.json`; local filled files are intentionally
  ignored.
- **Analytics layer:** `dbt/flight_fares/models/marts/`, `sql/analysis/`, and
  `analytics/`.
- **Local demo:** Docker/Postgres/Airflow sections lower in this README.

## Evidence

| Proof | Screenshot |
|---|---|
| ECR image tag `week9` pushed | [01-ecr-pushed-image-tag.png](docs/screenshots/week9/01-ecr-pushed-image-tag.png) |
| ECS cluster exists | [02-ecs-cluster.png](docs/screenshots/week9/02-ecs-cluster.png) |
| ECS task definition revision | [03-ecs-task-definition-revision.png](docs/screenshots/week9/03-ecs-task-definition-revision.png) |
| Manual Fargate task exit code `0` | [04-manual-fargate-task-exit-code-0.png](docs/screenshots/week9/04-manual-fargate-task-exit-code-0.png) |
| Manual CloudWatch success log | [05-cloudwatch-manual-run-week9-success.png](docs/screenshots/week9/05-cloudwatch-manual-run-week9-success.png) |
| EventBridge Scheduler enabled with ECS target | [06-eventbridge-scheduler-enabled-target.png](docs/screenshots/week9/06-eventbridge-scheduler-enabled-target.png) |
| Scheduled CloudWatch success log | [07-cloudwatch-scheduled-run-week9-success.png](docs/screenshots/week9/07-cloudwatch-scheduled-run-week9-success.png) |
| S3 Bronze prefix proof | [02-s3-bronze-prefix-view.png](docs/screenshots/week7/02-s3-bronze-prefix-view.png) |
| S3 manifest proof | [05-manifest-output-proof.png](docs/screenshots/week7/05-manifest-output-proof.png) |
| Redshift load success | [04-redshift-load-command-success.png](docs/screenshots/week8/04-redshift-load-command-success.png) |
| Redshift raw proof queries | [05-redshift-raw-load-proof-queries.png](docs/screenshots/week8/05-redshift-raw-load-proof-queries.png) |
| dbt Redshift debug success | [06-dbt-debug-redshift-success.png](docs/screenshots/week8/06-dbt-debug-redshift-success.png) |
| dbt Redshift build/tests success | [07-dbt-build-redshift-success.png](docs/screenshots/week8/07-dbt-build-redshift-success.png) |
| Redshift mart query proof | [08-redshift-mart-query-proof.png](docs/screenshots/week8/08-redshift-mart-query-proof.png) |

## Runbooks

- [Week 7: Real AWS Bronze S3 ingestion proof](docs/runbooks/week7_s3_bronze_ingestion.md)
- [Week 8: S3 -> Redshift Serverless -> dbt proof](docs/runbooks/week8_redshift_warehouse_proof.md)
- [Week 9: EventBridge Scheduler -> ECS/Fargate -> CloudWatch proof](docs/runbooks/week9_ecs_fargate_scheduler_proof.md)
- [Architecture detail](docs/architecture.md)

## Implemented vs Future Work

Implemented:
- Flight Fare API ingestion
- S3 Bronze load
- Redshift Serverless load
- dbt staging / marts / tests
- Docker batch runner
- ECS / Fargate execution
- EventBridge Scheduler trigger
- CloudWatch logging
- proof screenshots and runbooks

Future Work:
- Terraform / CloudFormation full IaC deployment
- MWAA or production orchestration expansion
- richer BI dashboard
- ML / buy-vs-wait extension
- monitoring and alerting improvements
- production hardening

## Cost and Secret Safety

- The recurring daily EventBridge schedule was disabled after proof collection.
- A one-time proof schedule was used for scheduled-run validation.
- AWS resources can incur cost if left running.
- Stop or delete unused ECS, ECR, CloudWatch, Redshift, and S3 resources when
  they are no longer needed.
- Local-only JSON files and secrets are ignored and should not be committed:
  `.env`, `dbt/profiles.yml`, and `aws/*.local.json`.
- Redshift password values are not committed; the ECS path uses Secrets Manager.

## Why This Project

Travel apps and planners struggle with "When should I book?" because fares
change by route, lead time, seasonality, and volatility. This pipeline produces
clean, tested tables that support route trends, price alerts, and buy/wait
analysis.

## Repo Structure

- `ingestion/` - Flight API ingestion and S3/local Bronze writers
- `warehouse/` - Redshift SQL runner and warehouse helpers
- `sql/redshift/` - Redshift schemas, COPY SQL, proof queries, and mart checks
- `dbt/flight_fares/` - dbt staging models, marts, tests, and macros
- `scripts/run_aws_batch_pipeline.py` - ECS/Fargate batch entrypoint
- `aws/` - AWS CLI templates; local filled JSON files are ignored
- `docs/runbooks/` - proof runbooks for Weeks 7-9
- `docs/screenshots/` - proof screenshots
- `airflow/` - Local Demo orchestration only
- `analytics/` and `ml/` - analytics outputs and optional modeling experiments

## Local Demo / Conceptual Pipeline

The local demo remains useful for development without AWS credentials. It uses
Docker/Postgres/dbt locally and is secondary to the Current Proven AWS Path.

![Local Demo / Conceptual Pipeline](docs/images/architecture_diagram.png)

### Quickstart: Local Demo In 10 Minutes

Prereqs:
- Python 3.11.x
- Docker Desktop
- dbt 1.7.x (`dbt-core` + `dbt-postgres`, installed via `requirements.txt`)

```bash
git clone https://github.com/rihua-tech/cloud-flight-fare-pipeline.git
cd cloud-flight-fare-pipeline
python -m venv .venv
pip install -r requirements.txt
docker compose up -d
python scripts/load_sample_to_postgres.py
dbt build --project-dir dbt/flight_fares --profiles-dir dbt
python scripts/run_analysis_queries.py
```

Note: `dbt/profiles.yml` is ignored because it can contain credentials. Use
`dbt/profiles.example.yml` as a template for local development.

### Local Airflow Demo

Week 5 includes a local Airflow DAG for scheduled local demo runs and retry/log
behavior. It is not the proven cloud orchestration path; the proven AWS trigger
is EventBridge Scheduler.

```bash
docker compose -f airflow/docker-compose.airflow.yml up -d --build
```

## Analytics-Ready Output

Primary mart tables:
- `marts.fact_fares`
- `marts.dim_route`
- `marts.dim_date`

Supporting documentation:
- `docs/how_to_use_marts.md`
- `docs/data_dictionary.md`
- `docs/kpi_definitions.md`

Dashboard preview:
- [docs/images/dashboard_screenshot.png](docs/images/dashboard_screenshot.png)

![Dashboard screenshot](docs/images/dashboard_screenshot.png)

<details>
<summary>Historical Milestone Details: Weeks 3–6</summary>

## Week 3 - S3 Bronze Ingestion

This step ingests daily fare snapshots and writes them to S3 in **bronze** partitioned folders.

### Command I ran (3 days)
```bash
python -m ingestion.ingest_api_to_s3 --mode s3 --start 2026-01-17 --days 3
```

### S3 path convention

Current Bronze layout (CSV):
```text
s3://<bucket>/bronze/dt=YYYY-MM-DD/fares.csv
```

*(Deprecated legacy layout: `bronze/flights/.../fares.jsonl`. This repo now uses one canonical Bronze layout: `bronze/dt=YYYY-MM-DD/fares.csv` for both ingestion and Redshift COPY.)*


Real examples (3 days):
- `s3://cloud-flight-fare-pipeline-rihua-2026-east1/bronze/dt=2026-01-22/fares.csv`
- `s3://cloud-flight-fare-pipeline-rihua-2026-east1/bronze/dt=2026-01-23/fares.csv`
- `s3://cloud-flight-fare-pipeline-rihua-2026-east1/bronze/dt=2026-01-24/fares.csv`

### Evidence (screenshots)

**Terminal output**
![Terminal upload](docs/screenshots/week3/terminal-upload.png)

**S3 console folders**
![S3 console](docs/screenshots/week3/s3-console.png)


## Week 4 - Warehouse Target (Summary)

**Option A (Local demo): Postgres "warehouse mode"**
1) docker compose up -d
2) python scripts/load_sample_to_postgres.py (or the psql files if you keep that method)
3) dbt build -t pg_warehouse
4) Link: warehouse/postgres_local.md (or docs/...)

**Option B (AWS): S3 -> Redshift Serverless -> dbt**
1) python warehouse/run_redshift_sql.py
2) dbt debug -t redshift
3) dbt build -t redshift
4) Link: docs/week4_redshift_runbook.md

## Week 4 - Warehouse target (S3 -> Redshift Serverless -> dbt)

This repo can run against **Redshift Serverless** as a second "warehouse target" (in addition to local Postgres demo).

**High-level flow**
1) Bronze file exists in S3 (CSV)
2) Run warehouse SQL helper to reset schemas + create `raw.fares` + `COPY` from S3
3) Run dbt against the `redshift` target (staging + marts + tests)
4) Run proof queries in a SQL client (Query Editor v2 / DBeaver)

**Commands (PowerShell)**
```powershell
# (Optional) dry-run prints rendered SQL (no changes in Redshift)
python warehouse/run_redshift_sql.py --dry-run

# Executes: reset schemas + create raw table + COPY from S3
python warehouse/run_redshift_sql.py

# dbt connectivity check
dbt debug --project-dir dbt/flight_fares --profiles-dir dbt -t redshift

# build models + run tests
dbt build --project-dir dbt/flight_fares --profiles-dir dbt -t redshift
```

**Proof queries (run in a SQL client, not PowerShell)**
```sql
select count(*) from raw.fares;
select count(*) from marts.fact_fares;
select * from marts.dim_route limit 10;
```

See `docs/week4_redshift_runbook.md` for full steps + "no secrets committed" notes.


---

## Week 5 - Orchestration (Airflow, Local)

Implemented a local Airflow orchestration demo for the **Cloud Flight Fare Pipeline** to satisfy the Week 5 requirement: **scheduled runs + retries + logs**.

### DAG implemented

**DAG ID:** `flight_fare_pipeline_local_demo`
**Schedule:** `@daily`
**Retries:** `2` (with `retry_delay=3 minutes`)
**Catchup:** `False`

### Task flow (end-to-end)

1. `load_sample_to_postgres` - loads sample fare data into local Postgres
2. `dbt_build` - runs `dbt deps` and `dbt build` (models + tests)
3. `run_analysis_queries` - runs proof/analysis SQL queries

### Run locally (Week 5 demo)

From the repo root:

```bash
# Start project Postgres (warehouse demo)
docker compose up -d

# Start Airflow services (webserver + scheduler + init)
docker compose -f airflow/docker-compose.airflow.yml up -d --build
```

Open Airflow UI:

* `http://localhost:8080`

### Logs and rerun behavior proof

* Task logs are captured in the **Airflow UI** (Task -> Logs)
* Rerun behavior was validated using **Clear and Retry** on a successful task (`run_analysis_queries`) in Graph view

### Week 5 screenshots

Saved under:

* `docs/screenshots/week5/`

Included proof:

* DAG list page
* Graph view (3 tasks connected)
* Task log output (`dbt_build`) with success
* Rerun proof (`Clear and Retry` dialog + rerun success)

---

## Week 6 - Documentation & Analytics Handoff

Portfolio handoff for reviewers: this section summarizes how to run and validate the pipeline.
Detailed implementation evidence remains in Weeks 3-5 above.

### Architecture

Detailed architecture: [docs/architecture.md](docs/architecture.md).

End-to-end flow:
1. Ingest fare snapshots to S3 bronze (or local demo input)
2. Transform raw records into curated warehouse-ready data
3. Load warehouse tables (local Postgres or AWS Redshift)
4. Build dbt marts for analytics consumption
5. Serve analytics queries and dashboard outputs

Core marts in this repo:
- `marts.fact_fares`
- `marts.dim_route`
- `marts.dim_date`

### Running Locally

Use this fast local path:

```bash
docker compose up -d
python scripts/load_sample_to_postgres.py
dbt build --project-dir dbt/flight_fares --profiles-dir dbt
python scripts/run_analysis_queries.py
```

For full setup context, see **Quickstart** and **Local demo (runs without AWS)** above.

### Running on AWS

AWS execution path (S3 + Redshift + dbt):

```bash
python warehouse/run_redshift_sql.py --dry-run
python warehouse/run_redshift_sql.py
dbt build --project-dir dbt/flight_fares --profiles-dir dbt -t redshift
```

For full production steps, see **Production notes (AWS/Redshift)** and `docs/week4_redshift_runbook.md`.

### Testing & CI

CI is automated with **GitHub Actions** in `.github/workflows/ci.yml` on pushes to `main` and pull requests.

Pipeline checks include:
- `ruff check .`
- `pytest -q`
- demo data load: `python scripts/load_sample_to_postgres.py`
- dbt validation: `dbt deps` + `dbt build`

### Example Queries & Outputs

Representative analytics queries run on the mart layer:

```sql
select
  origin || '-' || dest as route,
  avg(price_usd) as avg_price_usd
from marts.fact_fares
group by 1
order by avg_price_usd asc
limit 10;
```

```sql
select
  d.year,
  d.month,
  avg(f.price_usd) as monthly_avg_price_usd
from marts.fact_fares f
join marts.dim_date d on f.date_day = d.date_day
group by 1, 2
order by 1, 2;
```

Expected output categories:
- route-level fare trends
- monthly fare movement
- lead-time and weekday/weekend comparisons

Reference query sets:
- `sql/examples/`
- `sql/analysis/`

Sample materialized outputs:
- `analytics/outputs/`

### Analytics-ready Output (Week 6)

Week 6 deliverable is an analytics-ready mart layer for BI, ad-hoc SQL, and downstream modeling.

Primary output tables:
- `marts.fact_fares` (observed fare records with lead-time context)
- `marts.dim_route` (route dimension: origin, destination, route key)
- `marts.dim_date` (calendar dimension by snapshot date)

Supporting documentation:
- `docs/how_to_use_marts.md`
- `docs/data_dictionary.md`
- `docs/kpi_definitions.md`

### Dashboard Preview Artifact
- [docs/images/dashboard_screenshot.png](docs/images/dashboard_screenshot.png)

![Dashboard screenshot](docs/images/dashboard_screenshot.png)

---

</details>

## Real AWS Bronze S3 Ingestion Proof (Week 7)

Week 7 promotes Bronze ingestion to a clear real AWS S3 proof path while keeping
the local Postgres/dbt/Airflow demo unchanged.

Implemented:
- `ingestion.ingest_api_to_s3` supports explicit `--mode s3` runs.
- Multi-day Bronze CSV output writes to `s3://<bucket>/bronze/dt=YYYY-MM-DD/fares.csv`.
- Each run writes a manifest to `s3://<bucket>/bronze/_manifests/`.
- Manifest metadata includes run timestamp, dates processed, row counts per date,
  output S3 paths, partition statuses, and rerun behavior.
- Default rerun behavior is `overwrite`: reruns intentionally replace `fares.csv`
  at the same date partition path. Use `--rerun-behavior skip-existing` to leave
  existing partitions unchanged.

Run Week 7 against AWS S3:

```powershell
$env:AWS_REGION="us-east-1"
$env:AWS_PROFILE="<your-profile-name>"
$env:S3_BUCKET="<your-bucket-name>"
$env:S3_PREFIX_BRONZE="bronze"

python -m ingestion.ingest_api_to_s3 --mode s3 --start 2026-01-17 --days 3
```

Run 7 sample dates:

```powershell
python -m ingestion.ingest_api_to_s3 --mode s3 --start 2026-01-17 --days 7
```

Verify:

```powershell
aws s3 ls "s3://$env:S3_BUCKET/bronze/" --recursive
aws s3 ls "s3://$env:S3_BUCKET/bronze/_manifests/"
```

Save proof screenshots/logs under `docs/screenshots/week7/`:
- terminal output showing uploaded partitions and manifest path
- S3 console showing `bronze/dt=YYYY-MM-DD/` folders
- S3 console showing `bronze/_manifests/`
- manifest JSON preview with row counts, S3 paths, and rerun behavior
- optional rerun proof showing `overwritten` or `skipped_existing`

Full runbook: [docs/runbooks/week7_s3_bronze_ingestion.md](docs/runbooks/week7_s3_bronze_ingestion.md)

---

## Real AWS Warehouse Proof (Week 8)

Week 8 proves the AWS warehouse path: S3 Bronze CSV data loads into Redshift
Serverless, then dbt builds staging and marts with tests against the Redshift
target.

Implemented proof path:
- Redshift setup SQL creates `raw`, `staging`, `analytics`, and `marts` schemas.
- `raw.fares` is created for the canonical Bronze CSV layout.
- Redshift `COPY` loads from an exact `S3_COPY_URI`, including multi-day
  prefixes such as `s3://<bucket>/bronze/dt=`.
- Raw proof queries cover row count, min/max date, row count by date, sample
  rows, and key-column null checks.
- dbt Redshift target documentation explains `dbt debug -t redshift` and
  `dbt build -t redshift`.

Run the warehouse proof:

```powershell
$env:AWS_REGION="us-east-1"
$env:REDSHIFT_HOST="<workgroup>.<region>.redshift-serverless.amazonaws.com"
$env:REDSHIFT_PORT="5439"
$env:REDSHIFT_DBNAME="dev"
$env:REDSHIFT_USER="<redshift-user>"
$env:REDSHIFT_PASSWORD="<redshift-password>"
$env:REDSHIFT_SCHEMA_RAW="raw"
$env:IAM_ROLE_ARN="arn:aws:iam::<account-id>:role/<redshift-s3-copy-role>"
$env:S3_COPY_URI="s3://<bucket>/bronze/dt="

python warehouse/run_redshift_sql.py --dry-run
python warehouse/run_redshift_sql.py
python warehouse/run_redshift_sql.py --files 03_raw_load_proof_queries.sql
dbt debug --project-dir dbt/flight_fares --profiles-dir dbt -t redshift
dbt build --project-dir dbt/flight_fares --profiles-dir dbt -t redshift
```

Save proof screenshots/logs under `docs/screenshots/week8/`:
- Redshift Serverless workgroup/namespace and attached IAM role
- rendered COPY SQL or successful load log
- raw proof query output
- `dbt debug -t redshift` success
- `dbt build -t redshift` success with tests
- mart proof query output

Full runbook: [docs/runbooks/week8_redshift_warehouse_proof.md](docs/runbooks/week8_redshift_warehouse_proof.md)

---

## Real AWS Scheduled Pipeline Proof (Week 9)

Week 9 proves the scheduled AWS batch execution path:

```text
EventBridge Scheduler -> ECS / Fargate Batch Container -> Flight API Ingestion -> S3 Bronze -> Redshift Serverless -> dbt staging/marts/tests -> CloudWatch Logs
```

Implemented in this repo:
- `Dockerfile.batch` for building a Python/dbt batch image.
- `scripts/run_aws_batch_pipeline.py` as the ECS entrypoint.
- AWS CLI templates under `aws/` for ECR, ECS cluster, ECS task definition,
  manual Fargate run, EventBridge Scheduler, and CloudWatch log group.
- `.dockerignore` to keep local secrets, dbt profiles, generated data, and
  screenshots out of the Docker build context.

Completed proof:
- ECR image tag `week9` was pushed.
- ECS cluster `cloud-flight-fare-pipeline-week9` ran task definition
  `cloud-flight-fare-pipeline-batch:2`.
- Manual ECS/Fargate run completed with exit code `0`.
- EventBridge Scheduler triggered ECS/Fargate with the same task definition.
- Manual and scheduled CloudWatch log streams both showed
  `WEEK9_BATCH_SUCCESS`.
- A one-time proof schedule used `ActionAfterCompletion=DELETE`.
- The recurring daily schedule was disabled after proof collection to avoid
  unwanted daily AWS runs and cost.

The batch runner composes the existing Week 7/8 commands and generates a dbt
`flight_fares` profile at runtime from environment variables and ECS Secrets
Manager:

```powershell
python -m ingestion.ingest_api_to_s3 --mode s3
python warehouse/run_redshift_sql.py
dbt build --project-dir dbt/flight_fares --profiles-dir <runtime-profile-dir> -t redshift
python warehouse/run_redshift_sql.py --files verify_marts.sql
```

Save Week 9 screenshots/logs under:

```text
docs/screenshots/week9/
```

Expected proof screenshots:
- `01-ecr-pushed-image-tag.png`
- `02-ecs-cluster.png`
- `03-ecs-task-definition-revision.png`
- `04-manual-fargate-task-exit-code-0.png`
- `05-cloudwatch-manual-run-week9-success.png`
- `06-eventbridge-scheduler-enabled-target.png`
- `07-cloudwatch-scheduled-run-week9-success.png`

Full runbook: [docs/runbooks/week9_ecs_fargate_scheduler_proof.md](docs/runbooks/week9_ecs_fargate_scheduler_proof.md)

---

## Week 10 Documentation Closeout

Week 10 closes out the repository as a recruiter-ready AWS cloud data
engineering proof story. The README leads with the Current Proven AWS Path,
evidence screenshots, runbooks, implemented vs future work, and cost/secret
safety notes.

This closeout does not add new AWS infrastructure. It documents the completed
Week 7-9 proof path and makes the project easier to review quickly.
