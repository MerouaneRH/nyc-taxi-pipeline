"""Monthly NYC taxi ELT: ingest (zones, trips) -> dbt snapshot -> dbt build.

Each run discovers: it checks every month from `ingest_since` (default 2024-01)
through the current month and ingests any month that TLC has published but S3
lacks (or holds with a different size). Months not yet published return 403 and
are skipped; a later run picks them up. ingest_trips prints
`NEW_MONTHS=<months uploaded>` as its last line (XCom) and exits 99 when nothing
was uploaded; dbt_build builds each new month, oldest first. See ADR 0009.
Python/dbt run from a dedicated venv (/opt/venv) to avoid dependency clashes
with Airflow itself.
"""
import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PY = "/opt/venv/bin/python"
DBT = "/opt/venv/bin/dbt"
PROJECT = "/opt/project"
DBT_DIR = f"{PROJECT}/dbt"
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
# snapshot covers every year from the start month's year to the run's year (backfills)
VARS = (
    "--vars '{min_year: {{ since_year(params.ingest_since, data_interval_start) }}, "
    "max_year: {{ data_interval_start.year }}}'"
)
# ingest_trips' last stdout line is `NEW_MONTHS=2024-01 2024-02` (oldest first, may be empty)
NEW_MONTHS = "{{ ti.xcom_pull(task_ids='ingest_trips') or '' }}"

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def validated_since(since: str, run_month) -> str:
    """Return `since` if it is YYYY-MM and not after the run month; else raise."""
    if not isinstance(since, str) or not MONTH_RE.match(since):
        raise ValueError(f"ingest_since must be YYYY-MM, got {since!r}")
    if since > f"{run_month.year:04d}-{run_month.month:02d}":
        raise ValueError(f"ingest_since {since} is after the run month {run_month:%Y-%m}")
    return since


def since_year(since: str, run_month) -> int:
    return int(validated_since(since, run_month)[:4])


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
    user_defined_macros={"validated_since": validated_since, "since_year": since_year},
    params={
        "ingest_since": Param(
            "2024-01",
            type="string",
            pattern=MONTH_RE.pattern,
            description="First month (YYYY-MM) ingest_trips checks; must be <= the run month",
        )
    },
    tags=["nyc-taxi", "elt"],
) as dag:
    ingest_zones = _bash("ingest_zones", f"{PY} -m ingestion.pipeline --zones")
    ingest_trips = _bash(
        "ingest_trips",
        f"{PY} -m ingestion.pipeline --since "
        "{{ validated_since(params.ingest_since, data_interval_start) }} "
        "--service yellow --skip-unpublished",
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
