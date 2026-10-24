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
    assert dag.schedule_interval == "@monthly"
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


def test_window_param_definition(dagbag):
    param = dagbag.dags[DAG_ID].params.get_param("ingest_window_months")
    assert param.value == 4
    assert param.schema["minimum"] == 1 and param.schema["maximum"] == 12


def _render(dagbag, task_id: str, start=(2024, 3), window=None, xcom=None) -> str:
    dag = dagbag.dags[DAG_ID]
    task = dag.get_task(task_id)
    params = {"ingest_window_months": dag.params["ingest_window_months"] if window is None else window}
    ctx = {
        "data_interval_start": pendulum.datetime(*start, 1, tz="UTC"),
        "params": params,
        "macros": macros,  # same object Airflow puts in the task context
        "ti": SimpleNamespace(xcom_pull=lambda task_ids=None: xcom),
    }
    return task.render_template(task.bash_command, ctx)


def _ingest_months(cmd: str) -> list[str]:
    args = shlex.split(cmd)
    i = args.index("--months") + 1
    j = next(k for k in range(i, len(args)) if args[k].startswith("--"))
    return args[i:j]


@pytest.mark.parametrize(
    "start, window, expected",
    [
        ((2024, 2), None, ["2024-02", "2024-01", "2023-12", "2023-11"]),
        ((2024, 3), None, ["2024-03", "2024-02", "2024-01", "2023-12"]),
        ((2024, 12), None, ["2024-12", "2024-11", "2024-10", "2024-09"]),
        ((2024, 3), 1, ["2024-03"]),
        ((2025, 1), 12, [
            "2025-01", "2024-12", "2024-11", "2024-10", "2024-09", "2024-08",
            "2024-07", "2024-06", "2024-05", "2024-04", "2024-03", "2024-02",
        ]),
    ],
)
def test_rendered_ingest_trips_window_newest_first(dagbag, start, window, expected):
    cmd = _render(dagbag, "ingest_trips", start, window)
    assert _ingest_months(cmd) == expected
    assert "--service yellow" in cmd and "--skip-unpublished" in cmd
    assert "-m ingestion.pipeline" in cmd


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
