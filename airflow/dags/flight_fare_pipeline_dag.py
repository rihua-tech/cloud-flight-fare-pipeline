"""Airflow DAG outline for the Cloud Flight Fare Pipeline.

This repo includes a runnable **local demo** (Postgres + dbt).
This DAG shows how you would orchestrate the same flow in production (MWAA).

Prod idea:
1) ingest API -> S3
2) COPY S3 -> Redshift raw/staging
3) dbt build (staging + marts + tests)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cloud_flight_fare_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["data", "aws", "dbt"],
) as dag:
    start = EmptyOperator(task_id="start")

    ingest = BashOperator(
        task_id="ingest_to_bronze",
        bash_command="python -m ingestion.ingest_api_to_s3 --date {{ ds }} --to-s3",
    )

    copy_to_redshift = EmptyOperator(task_id="copy_s3_to_redshift")

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd dbt/flight_fares && dbt build",
    )

    end = EmptyOperator(task_id="end")

    start >> ingest >> copy_to_redshift >> dbt_build >> end
