"""DAG import + structure tests for nyc_taxi_elt."""
import shlex
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pendulum
import pytest
import yaml

pytest.importorskip("airflow")
from airflow import macros
from airflow.exceptions import ParamValidationError
from airflow.models import DagBag

DAGS = Path(__file__).resolve().parents[2] / "dags"
DAG_ID = "nyc_taxi_elt"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_task_ids(dagbag):
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None
    assert set(dag.task_ids) == {
        "ingest_zones", "ingest_trips", "dbt_prepare", "dbt_snapshot", "dbt_build",
    }


def test_dependency_order(dagbag):
    dag = dagbag.dags.get(DAG_ID)
    down = {t: set(dag.get_task(t).downstream_task_ids) for t in dag.task_ids}
    assert down["ingest_zones"] == {"ingest_trips"}
    assert down["ingest_trips"] == {"dbt_prepare"}
    assert down["dbt_prepare"] == {"dbt_snapshot"}
    assert down["dbt_snapshot"] == {"dbt_build"}
    assert down["dbt_build"] == set()


def test_schedule_and_catchup_bounded(dagbag):
    dag = dagbag.dags.get(DAG_ID)
    assert dag.catchup is False
    assert dag.schedule_interval == "0 */6 * * *"
    assert (dag.start_date.year, dag.start_date.month) == (2024, 1)
    assert dag.max_active_runs == 1


def test_ingest_trips_idempotent_and_retries(dagbag):
    t = dagbag.dags.get(DAG_ID).get_task("ingest_trips")
    assert t.retries >= 2
    assert "--service yellow" in t.bash_command


def test_ingest_trips_skips_when_nothing_new(dagbag):
    t = dagbag.dags[DAG_ID].get_task("ingest_trips")
    assert "--skip-unpublished" in t.bash_command
    assert t.skip_on_exit_code == [99]


def test_since_param_definition(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert "ingest_window_months" not in dag.params
    param = dag.params.get_param("ingest_since")
    assert param.value == "2024-01"
    assert param.schema["type"] == "string"


def _render(
    dagbag, task_id: str, start=(2024, 3), since="2024-01", xcom=None, until=""
) -> str:
    dag = dagbag.dags[DAG_ID]
    task = dag.get_task(task_id)
    ctx = {
        "data_interval_start": pendulum.datetime(*start, 1, tz="UTC"),
        "params": {"ingest_since": since, "ingest_until": until},
        "macros": macros,  # same object Airflow puts in the task context
        "ti": SimpleNamespace(xcom_pull=lambda task_ids=None: xcom),
    }
    return task.render_template(task.bash_command, ctx)


@pytest.mark.parametrize("since", ["2024-01", "2025-06", "2026-10"])
def test_rendered_ingest_trips_uses_since(dagbag, since):
    cmd = _render(dagbag, "ingest_trips", (2026, 10), since)
    args = shlex.split(cmd)
    assert args[args.index("--since") + 1] == since
    assert "--months" not in args
    assert "--service yellow" in cmd and "--skip-unpublished" in cmd
    assert "-m ingestion.pipeline" in cmd


@pytest.mark.parametrize("since", ["2024-1", "24-01", "2024/01", "2024-13", "2024-00", "", "latest", "2024-01 --zones"])
def test_since_bad_format_rejected(dagbag, since):
    with pytest.raises(ValueError, match="YYYY-MM"):
        _render(dagbag, "ingest_trips", (2026, 10), since)


def test_since_future_month_rejected(dagbag):
    with pytest.raises(ValueError, match="after the run month"):
        _render(dagbag, "ingest_trips", (2024, 3), "2024-04")
    assert "2024-03" in _render(dagbag, "ingest_trips", (2024, 3), "2024-03")


def test_since_param_schema_rejects_bad_format(dagbag):
    param = dagbag.dags[DAG_ID].params.get_param("ingest_since")
    with pytest.raises(ParamValidationError):
        param.resolve("2024-13")
    with pytest.raises(ParamValidationError):
        param.resolve("january")
    assert param.resolve("2025-07") == "2025-07"


def _vars(cmd: str) -> dict:
    raw = cmd.split("--vars ", 1)[1]
    assert raw.startswith("'") and "'" in raw[1:]
    return yaml.safe_load(raw[1:].split("'", 1)[0])


def _run_build(cmd: str, tmp_path) -> list[list[str]]:
    """Run the rendered dbt_build command with a stub dbt; return each invocation."""
    stub = tmp_path / "dbt"
    stub.write_text('#!/bin/bash\nprintf "%s\\0" "$@"; printf "\\n--END--\\n"\n')
    stub.chmod(0o755)
    out = subprocess.run(
        ["bash", "-c", cmd.replace("/opt/venv/bin/dbt", str(stub))],
        capture_output=True, text=True, check=True,
    ).stdout
    calls = [c for c in out.split("\n--END--\n") if c.strip("\n")]
    return [c.strip("\n").split("\0")[:-1] if c.endswith("\0") else c.split("\0") for c in calls]


def _call_vars(call: list[str]) -> dict:
    return yaml.safe_load(call[call.index("--vars") + 1])


def test_dbt_build_one_month(dagbag, tmp_path):
    cmd = _render(dagbag, "dbt_build", xcom="NEW_MONTHS=2024-02")
    calls = _run_build(cmd, tmp_path)
    assert len(calls) == 1
    assert calls[0][0] == "build"
    sel = calls[0][calls[0].index("--select") + 1 :][:2]
    assert sel == ["stg_zones+", "stg_trips+"]
    assert _call_vars(calls[0]) == {"min_year": 2024, "max_year": 2024, "run_month": "2024-02"}


def test_dbt_build_two_months_oldest_first_own_year(dagbag, tmp_path):
    cmd = _render(dagbag, "dbt_build", xcom="NEW_MONTHS=2023-12 2024-01")
    calls = _run_build(cmd, tmp_path)
    assert [_call_vars(c) for c in calls] == [
        {"min_year": 2023, "max_year": 2023, "run_month": "2023-12"},
        {"min_year": 2024, "max_year": 2024, "run_month": "2024-01"},
    ]


@pytest.mark.parametrize("xcom", ["NEW_MONTHS=", "", None])
def test_dbt_build_empty_list_is_noop(dagbag, tmp_path, xcom):
    cmd = _render(dagbag, "dbt_build", xcom=xcom)
    assert _run_build(cmd, tmp_path) == []


def test_dbt_build_uses_xcom_of_ingest_trips(dagbag):
    cmd = dagbag.dags[DAG_ID].get_task("dbt_build").bash_command
    assert "ti.xcom_pull(task_ids='ingest_trips')" in cmd and "NEW_MONTHS=" in cmd


def test_rendered_snapshot_vars_parse_as_yaml(dagbag):
    assert _vars(_render(dagbag, "dbt_snapshot")) == {"min_year": 2024, "max_year": 2024}


def test_snapshot_vars_cover_backfill_years(dagbag):
    cmd = _render(dagbag, "dbt_snapshot", (2026, 10), "2024-01")
    assert _vars(cmd) == {"min_year": 2024, "max_year": 2026}


def test_snapshot_vars_since_in_run_year(dagbag):
    cmd = _render(dagbag, "dbt_snapshot", (2026, 10), "2026-03")
    assert _vars(cmd) == {"min_year": 2026, "max_year": 2026}


def test_dbt_build_backfill_each_month_own_year(dagbag, tmp_path):
    xcom = "NEW_MONTHS=2024-05 2024-12 2025-01 2026-08"
    calls = _run_build(_render(dagbag, "dbt_build", (2026, 10), xcom=xcom), tmp_path)
    assert [_call_vars(c) for c in calls] == [
        {"min_year": 2024, "max_year": 2024, "run_month": "2024-05"},
        {"min_year": 2024, "max_year": 2024, "run_month": "2024-12"},
        {"min_year": 2025, "max_year": 2025, "run_month": "2025-01"},
        {"min_year": 2026, "max_year": 2026, "run_month": "2026-08"},
    ]


# --- ingest_until: bounded batches ---


def test_until_param_definition(dagbag):
    param = dagbag.dags[DAG_ID].params.get_param("ingest_until")
    assert param.value == ""
    assert param.schema["type"] == "string"
    assert param.resolve("2024-12") == "2024-12"
    assert param.resolve("") == ""
    with pytest.raises(ParamValidationError):
        param.resolve("2024-13")


def test_rendered_ingest_trips_with_until(dagbag):
    args = shlex.split(_render(dagbag, "ingest_trips", (2026, 10), "2024-01", until="2024-12"))
    assert args[args.index("--since") + 1] == "2024-01"
    assert args[args.index("--until") + 1] == "2024-12"
    assert "--skip-unpublished" in args and "--months" not in args


@pytest.mark.parametrize("until", ["", None])
def test_rendered_ingest_trips_without_until_when_empty(dagbag, until):
    cmd = _render(dagbag, "ingest_trips", (2026, 10), "2024-01", until=until)
    assert "--until" not in cmd
    assert "--since 2024-01" in cmd and "--skip-unpublished" in cmd


@pytest.mark.parametrize("until", ["2024-1", "24-12", "2024/12", "2024-13", "2024-00", "dec", "2024-12 --zones"])
def test_until_bad_format_rejected(dagbag, until):
    with pytest.raises(ValueError, match="YYYY-MM"):
        _render(dagbag, "ingest_trips", (2026, 10), "2024-01", until=until)


def test_until_before_since_rejected_equal_ok(dagbag):
    with pytest.raises(ValueError, match="before ingest_since"):
        _render(dagbag, "ingest_trips", (2026, 10), "2024-06", until="2024-05")
    assert "--until 2024-06" in _render(dagbag, "ingest_trips", (2026, 10), "2024-06", until="2024-06")
