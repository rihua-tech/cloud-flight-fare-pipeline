FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
  && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/airflow

RUN pip install --no-cache-dir -U pip
RUN pip install --no-cache-dir "dbt-core>=1.7,<1.8" "dbt-postgres>=1.7,<1.8"
