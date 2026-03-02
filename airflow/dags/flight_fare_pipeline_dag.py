"""Airflow DAG outline for the Cloud Flight Fare Pipeline.

This repo includes a runnable **local demo** (Postgres + dbt).
This DAG shows how you would orchestrate the same flow in production (MWAA).

Prod idea:
1) ingest API -> S3
2) COPY S3 -> Redshift raw/staging
3) dbt build (staging + marts + tests)
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"

DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt/flight_fares"
DBT_PROFILES_DIR = f"{PROJECT_DIR}/dbt"

default_args = {
    "owner": "rihua-tech",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="flight_fare_pipeline_local_demo",
    description="Local demo: load sample -> dbt build -> proof queries (Week 5 Airflow orchestration)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["cloud-flight-fare-pipeline", "week5", "local-demo"],
) as dag:

    load_sample_to_postgres = BashOperator(
        task_id="load_sample_to_postgres",
        bash_command=f"""
        set -euo pipefail
        cd {PROJECT_DIR}
        python scripts/load_sample_to_postgres.py
        """,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"""
        set -euo pipefail
        cd {PROJECT_DIR}
        dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}
        dbt build --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    run_analysis_queries = BashOperator(
        task_id="run_analysis_queries",
        bash_command=f"""
        set -euo pipefail
        cd {PROJECT_DIR}
        python scripts/run_analysis_queries.py
        """,
    )

    load_sample_to_postgres >> dbt_build >> run_analysis_queries