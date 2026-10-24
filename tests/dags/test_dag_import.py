"""DAG import + structure tests for nyc_taxi_elt."""
from pathlib import Path

import pendulum
import pytest
import yaml

pytest.importorskip("airflow")
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
    assert "{{ data_interval_start.strftime('%Y-%m') }}" in t.bash_command
    assert "--service yellow" in t.bash_command


def test_dbt_build_is_incremental_for_the_month(dagbag):
    cmd = dagbag.dags["nyc_taxi_elt"].get_task("dbt_build").bash_command
    select = cmd.split("--select ", 1)[1].split(" --vars")[0]
    assert select.split() == ["stg_zones+", "stg_trips+"]
    assert "run_month" in cmd and "data_interval_start.strftime('%Y-%m')" in cmd


def test_ingest_trips_skips_when_month_unpublished(dagbag):
    t = dagbag.dags[DAG_ID].get_task("ingest_trips")
    assert "--skip-unpublished" in t.bash_command
    assert t.skip_on_exit_code == [99]


def _render(dagbag, task_id: str) -> str:
    task = dagbag.dags[DAG_ID].get_task(task_id)
    ctx = {"data_interval_start": pendulum.datetime(2024, 3, 1, tz="UTC")}
    return task.render_template(task.bash_command, ctx)


def _vars(cmd: str) -> dict:
    raw = cmd.split("--vars ", 1)[1]
    assert raw.startswith("'") and "'" in raw[1:]
    return yaml.safe_load(raw[1:].split("'", 1)[0])


def test_rendered_ingest_trips_month(dagbag):
    cmd = _render(dagbag, "ingest_trips")
    assert "--months 2024-03 " in cmd and "--skip-unpublished" in cmd


def test_rendered_build_vars_parse_as_yaml(dagbag):
    assert _vars(_render(dagbag, "dbt_build")) == {
        "min_year": 2024, "max_year": 2024, "run_month": "2024-03",
    }


def test_rendered_snapshot_vars_parse_as_yaml(dagbag):
    assert _vars(_render(dagbag, "dbt_snapshot")) == {"min_year": 2024, "max_year": 2024}
