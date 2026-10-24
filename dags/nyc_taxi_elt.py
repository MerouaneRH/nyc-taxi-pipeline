"""Monthly NYC taxi ELT: ingest (zones, trips) -> dbt snapshot -> dbt build.

Each run processes one logical month (data_interval_start). Ingestion is
expected to be idempotent per month, so retries and re-runs are safe.
Python/dbt run from a dedicated venv (/opt/venv) to avoid dependency clashes
with Airflow itself.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PY = "/opt/venv/bin/python"
DBT = "/opt/venv/bin/dbt"
PROJECT = "/opt/project"
DBT_DIR = f"{PROJECT}/dbt"
MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"
YEAR = "{{ data_interval_start.year }}"
VARS = f"--vars '{{min_year: {YEAR}, max_year: {YEAR}}}'"
# incremental: only this month's partition of fct_trips is rewritten
BUILD_VARS = f"--vars '{{min_year: {YEAR}, max_year: {YEAR}, run_month: \"{MONTH}\"}}'"

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def _bash(task_id: str, cmd: str, **kw) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=cmd, cwd=PROJECT, **kw)


with DAG(
    dag_id="nyc_taxi_elt",
    description="NYC taxi: S3 raw ingestion then dbt-athena snapshot/build",
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nyc-taxi", "elt"],
) as dag:
    ingest_zones = _bash("ingest_zones", f"{PY} -m ingestion.pipeline --zones")
    ingest_trips = _bash(
        "ingest_trips",
        f"{PY} -m ingestion.pipeline --months {MONTH} --service yellow",
    )
    dbt_cmd = f"{DBT} {{sub}} --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target athena"
    # idempotent setup: raw external tables (CREATE ... IF NOT EXISTS) and the vendors seed
    dbt_prepare = _bash(
        "dbt_prepare",
        f"{dbt_cmd.format(sub='run-operation create_raw_tables')}"
        f" && {dbt_cmd.format(sub='seed')}",
    )
    dbt_snapshot = _bash("dbt_snapshot", f"{dbt_cmd.format(sub='snapshot')} {VARS}")
    dbt_build = _bash(
        "dbt_build",
        f"{dbt_cmd.format(sub='build')} --select stg_trips+ {BUILD_VARS}",
    )

    ingest_zones >> ingest_trips >> dbt_prepare >> dbt_snapshot >> dbt_build
