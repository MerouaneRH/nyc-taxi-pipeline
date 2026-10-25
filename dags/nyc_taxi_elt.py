"""NYC taxi ELT, polled every 6 hours: ingest (zones, trips) -> dbt snapshot -> dbt build.

Each run discovers: it checks every month from `ingest_since` (default 2024-01)
through `ingest_until` (default empty = the current month) and ingests any month
that TLC has published but S3 lacks (or holds with a different size). Months not
yet published return 403 and are skipped; a later run picks them up.
ingest_trips prints `NEW_MONTHS=<months uploaded>` as its last line (XCom) and
exits 99 when nothing was uploaded, which skips dbt; dbt_build builds each new
month, oldest first. notify_new pushes an ntfy message when months were loaded and
a failure callback pushes one per failed task; topic = Airflow Variable NTFY_TOPIC.
See ADR 0009 and 0010.
Python/dbt run from a dedicated venv (/opt/venv) to avoid dependency clashes
with Airflow itself.
"""
import logging
import re
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from jinja2 import Undefined

log = logging.getLogger(__name__)

PY = "/opt/venv/bin/python"
DBT = "/opt/venv/bin/dbt"
PROJECT = "/opt/project"
DBT_DIR = f"{PROJECT}/dbt"
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
NTFY_URL = "https://ntfy.sh"
NTFY_TIMEOUT = 10
TOPIC_RE = re.compile(r"^[A-Za-z0-9_-]+$")
NOTIFY_TITLE = "NYC taxi ELT"
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
    "on_failure_callback": None,  # set below, once notify_failure is defined
}


def validated_since(since: str, run_month) -> str:
    """Return `since` if it is YYYY-MM and not after the run month; else raise."""
    if not isinstance(since, str) or not MONTH_RE.match(since):
        raise ValueError(f"ingest_since must be YYYY-MM, got {since!r}")
    if since > f"{run_month.year:04d}-{run_month.month:02d}":
        raise ValueError(f"ingest_since {since} is after the run month {run_month:%Y-%m}")
    return since


def until_flag(since: str, until) -> str:
    """Return `--until <until>` for a valid `until` >= `since`, or '' when unset."""
    if until is None or until == "" or isinstance(until, Undefined):
        return ""
    if not isinstance(until, str) or not MONTH_RE.match(until):
        raise ValueError(f"ingest_until must be YYYY-MM or empty, got {until!r}")
    if until < since:
        raise ValueError(f"ingest_until {until} is before ingest_since {since}")
    return f"--until {until}"


def since_year(since: str, run_month) -> int:
    return int(validated_since(since, run_month)[:4])


def notify(title: str, message: str, priority: str = "default") -> bool:
    """POST to ntfy.sh/<NTFY_TOPIC>. Never raises; returns True only if sent."""
    try:
        topic = (Variable.get("NTFY_TOPIC", default_var="") or "").strip()
        if not TOPIC_RE.match(topic):
            log.info("ntfy: NTFY_TOPIC unset or invalid, not sending")
            return False
        resp = requests.post(
            f"{NTFY_URL}/{topic}",
            data=message.encode(),
            headers={"Title": title, "Priority": priority},
            timeout=NTFY_TIMEOUT,
        )
        resp.raise_for_status()
        return True
    except Exception as exc:  # noqa: BLE001 - a notification must never fail a task
        log.warning("ntfy: send failed (%s)", type(exc).__name__)
        return False


def notify_failure(context) -> None:
    """on_failure_callback: high-priority message with task id and run id only."""
    try:
        task_id = context["task_instance"].task_id
        run_id = context["run_id"]
        notify(NOTIFY_TITLE, f"{NOTIFY_TITLE}: task {task_id} failed in run {run_id}", "high")
    except Exception as exc:  # noqa: BLE001
        log.warning("ntfy: failure callback error (%s)", type(exc).__name__)


def notify_loaded(ti, dag_run, **_) -> None:
    """Announce months loaded by this run; silent if none or dbt_build did not succeed."""
    try:
        months = [m for m in (ti.xcom_pull(task_ids="ingest_trips") or "").replace(
            "NEW_MONTHS=", "").split() if MONTH_RE.match(m)]
        dbt = dag_run.get_task_instance("dbt_build")
        if months and dbt is not None and str(dbt.state) == "success":
            notify(NOTIFY_TITLE, f"{NOTIFY_TITLE}: loaded {', '.join(months)}, built in dbt")
    except Exception as exc:  # noqa: BLE001
        log.warning("ntfy: notify_new error (%s)", type(exc).__name__)


default_args["on_failure_callback"] = notify_failure


def _bash(task_id: str, cmd: str, **kw) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=cmd, cwd=PROJECT, **kw)


with DAG(
    dag_id="nyc_taxi_elt",
    description="NYC taxi: S3 raw ingestion then dbt-athena snapshot/build",
    schedule="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    user_defined_macros={
        "validated_since": validated_since,
        "since_year": since_year,
        "until_flag": until_flag,
    },
    params={
        "ingest_since": Param(
            "2024-01",
            type="string",
            pattern=MONTH_RE.pattern,
            description="First month (YYYY-MM) ingest_trips checks; must be <= the run month",
        ),
        "ingest_until": Param(
            "",
            type="string",
            pattern=r"^(\d{4}-(0[1-9]|1[0-2]))?$",
            description="Last month (YYYY-MM, inclusive) to check, >= ingest_since; "
            "empty = through the current month",
        ),
    },
    tags=["nyc-taxi", "elt"],
) as dag:
    ingest_zones = _bash("ingest_zones", f"{PY} -m ingestion.pipeline --zones")
    ingest_trips = _bash(
        "ingest_trips",
        f"{PY} -m ingestion.pipeline --since "
        "{{ validated_since(params.ingest_since, data_interval_start) }} "
        "--service yellow --skip-unpublished "
        "{{ until_flag(params.ingest_since, params.ingest_until) }}",
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

    # all_done: runs even when dbt_build was skipped; notify_loaded checks its state itself
    notify_new = PythonOperator(
        task_id="notify_new",
        python_callable=notify_loaded,
        trigger_rule="all_done",
        retries=0,
    )

    ingest_zones >> ingest_trips >> dbt_prepare >> dbt_snapshot >> dbt_build >> notify_new
