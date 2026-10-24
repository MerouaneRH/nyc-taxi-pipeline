from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from app import data


def _client(rows, state="SUCCEEDED", reason=""):
    c = MagicMock()
    c.start_query_execution.return_value = {"QueryExecutionId": "q1"}
    c.get_query_execution.return_value = {
        "QueryExecution": {
            "Status": {"State": state, "StateChangeReason": reason},
            "Statistics": {"DataScannedInBytes": 1234},
        }
    }
    cols = [{"Label": n, "Type": t} for n, t in rows["cols"]]
    header = {"Data": [{"VarCharValue": n} for n, _ in rows["cols"]]}
    body = [{"Data": [{"VarCharValue": v} for v in r]} for r in rows["data"]]
    page = {"ResultSet": {"ResultSetMetadata": {"ColumnInfo": cols}, "Rows": [header, *body]}}
    c.get_paginator.return_value.paginate.return_value = [page]
    return c


def test_validate_range_rejects_reversed():
    with pytest.raises(ValueError):
        data.validate_range(date(2024, 2, 1), date(2024, 1, 1))


def test_validate_range_rejects_too_wide():
    with pytest.raises(ValueError):
        data.validate_range(date(2020, 1, 1), date(2024, 1, 1))


def test_validate_range_rejects_non_date():
    with pytest.raises(ValueError):
        data.validate_range("2024-01-01'; drop table x;--", date(2024, 1, 2))


@pytest.mark.parametrize("bad", ["Manhattan'; drop", "", "a" * 60, "Queens;--"])
def test_validate_borough_rejects(bad):
    with pytest.raises(ValueError):
        data.validate_borough(bad)


def test_validate_borough_accepts_and_none():
    assert data.validate_borough("Staten Island") == "Staten Island"
    assert data.validate_borough(None) is None
    assert data.validate_borough("All") is None


def test_validate_limit_bounds():
    assert data.validate_limit(10) == 10
    for bad in (0, -1, 10_000, "5; drop", True):
        with pytest.raises(ValueError):
            data.validate_limit(bad)


def test_daily_query_parameterised_and_limited():
    sql, params = data.build_daily_query(date(2024, 1, 1), date(2024, 1, 31), "Queens")
    assert "?" in sql and "Queens" not in sql and "2024" not in sql
    assert "limit" in sql.lower()
    assert params == ["'2024-01-01'", "'2024-01-31'", "'Queens'"]


def test_queries_without_borough_have_no_borough_param():
    for build in (data.build_daily_query, data.build_heatmap_query):
        sql, params = build(date(2024, 1, 1), date(2024, 1, 2), None)
        assert len(params) == 2 and "borough" not in sql.lower().split("where")[1]


def test_all_queries_have_limit():
    d1, d2 = date(2024, 1, 1), date(2024, 1, 2)
    qs = [
        data.build_daily_query(d1, d2, None)[0],
        data.build_zones_query(d1, d2, None, 10)[0],
        data.build_heatmap_query(d1, d2, None)[0],
        data.BOROUGHS_SQL,
        data.BOUNDS_SQL,
    ]
    assert all("limit" in q.lower() for q in qs)


def test_heatmap_filters_partition_date_in_fact_table():
    sql, _ = data.build_heatmap_query(date(2024, 1, 1), date(2024, 1, 2), None)
    assert "fct_trips" in sql and "pickup_date between" in sql


def test_run_query_returns_typed_dataframe():
    c = _client({"cols": [("d", "date"), ("trips", "bigint"), ("rev", "double"), ("z", "varchar")],
                 "data": [["2024-01-01", "5", "10.5", "JFK"]]})
    df = data.run_query(c, "select 1 limit 1", ["'x'"])
    kw = c.start_query_execution.call_args.kwargs
    assert kw["WorkGroup"] == "nyc-taxi-dbt" and kw["ExecutionParameters"] == ["'x'"]
    assert kw["ResultConfiguration"]["OutputLocation"].startswith("s3://")
    assert df.loc[0, "trips"] == 5 and df.loc[0, "rev"] == 10.5 and df.loc[0, "z"] == "JFK"
    assert df.attrs["scanned_bytes"] == 1234


def test_run_query_failure_raises():
    c = _client({"cols": [("a", "varchar")], "data": []}, state="FAILED", reason="boom")
    with pytest.raises(data.QueryError, match="boom"):
        data.run_query(c, "select 1 limit 1")


def test_run_query_timeout(monkeypatch):
    c = _client({"cols": [("a", "varchar")], "data": []}, state="RUNNING")
    monkeypatch.setattr(data, "POLL_SECONDS", 0)
    monkeypatch.setattr(data, "MAX_POLLS", 3)
    with pytest.raises(data.QueryError, match="timed out"):
        data.run_query(c, "select 1 limit 1")
    c.stop_query_execution.assert_called_once_with(QueryExecutionId="q1")


def test_kpis():
    df = pd.DataFrame({"trips": [10, 10], "revenue": [100.0, 100.0],
                       "fare_revenue": [80.0, 80.0], "tip_revenue": [8.0, 8.0]})
    k = data.compute_kpis(df)
    assert k == {"trips": 20, "revenue": 200.0, "avg_tip_pct": 10.0}
    assert data.compute_kpis(df.iloc[0:0])["avg_tip_pct"] == 0.0
