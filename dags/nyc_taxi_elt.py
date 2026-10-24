"""Monthly NYC taxi ELT: ingest (zones, trips) -> dbt snapshot -> dbt build.

Each run tries a window of the last `ingest_window_months` months ending at
data_interval_start (newest first). TLC publishes a month about two months late,
so a later run picks up a month an earlier run found unpublished. Ingestion skips
months already in S3 with the same size, so re-trying them is cheap and safe.
ingest_trips prints `NEW_MONTHS=<months uploaded>` as its last line (XCom) and
exits 99 when nothing was uploaded; dbt_build builds each new month.
Python/dbt run from a dedicated venv (/opt/venv) to avoid dependency clashes
with Airflow itself.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PY = "/opt/venv/bin/python"
DBT = "/opt/venv/bin/dbt"
PROJECT = "/opt/project"
DBT_DIR = f"{PROJECT}/dbt"
YEAR = "{{ data_interval_start.year }}"
VARS = f"--vars '{{min_year: {YEAR}, max_year: {YEAR}}}'"
# newest first: data_interval_start, then one month back per step, across years
WINDOW = (
    "{% for i in range(params.ingest_window_months) %}"
    "{{ (data_interval_start - macros.dateutil.relativedelta.relativedelta(months=i))"
    ".strftime('%Y-%m') }} {% endfor %}"
)
# ingest_trips' last stdout line is `NEW_MONTHS=2024-01 2024-02` (oldest first, may be empty)
NEW_MONTHS = "{{ ti.xcom_pull(task_ids='ingest_trips') or '' }}"

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
    params={
        "ingest_window_months": Param(
            4,
            type="integer",
            minimum=1,
            maximum=12,
            description="How many months back (from the run month) ingest_trips tries",
        )
    },
    tags=["nyc-taxi", "elt"],
) as dag:
    ingest_zones = _bash("ingest_zones", f"{PY} -m ingestion.pipeline --zones")
    ingest_trips = _bash(
        "ingest_trips",
        f"{PY} -m ingestion.pipeline --months {WINDOW}--service yellow --skip-unpublished",
        # exit 99 = nothing uploaded (all current or unpublished): skip dbt, don't fail
        skip_on_exit_code=99,
    )
    dbt_cmd = f"{DBT} {{sub}} --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target athena"
    # idempotent setup: raw external tables (CREATE ... IF NOT EXISTS) and the vendors seed
    dbt_prepare = _bash(
        "dbt_prepare",
        f"{dbt_cmd.format(sub='run-operation create_raw_tables')}"
        f" && {dbt_cmd.format(sub='seed')}",
    )
    dbt_snapshot = _bash("dbt_snapshot", f"{dbt_cmd.format(sub='snapshot')} {VARS}")
    # one incremental build per newly uploaded month, oldest first; empty list = no-op
    dbt_build = _bash(
        "dbt_build",
        f'for M in $(echo "{NEW_MONTHS}" | sed "s/^NEW_MONTHS=//"); do '
        f"{dbt_cmd.format(sub='build')} --select stg_zones+ stg_trips+ "
        '--vars "{min_year: ${M%%-*}, max_year: ${M%%-*}, run_month: \\"$M\\"}" '
        "|| exit 1; done",
    )

    ingest_zones >> ingest_trips >> dbt_prepare >> dbt_snapshot >> dbt_build
