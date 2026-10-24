"""DAG import + structure tests for nyc_taxi_elt."""
from pathlib import Path

import pytest

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
