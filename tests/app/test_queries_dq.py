from datetime import date

import pytest

from app import dq_reasons, queries, queries_dq

S, E = date(2024, 1, 5), date(2024, 2, 10)


def test_quality_query_has_new_columns():
    sql, params = queries.build_quality_query(S, E)
    for col in ("dropped_negative_total", "negative_total_sum", "dropped_dropoff_before_pickup",
                "dropped_duration_ge_24h", "dropped_pickup_outside_month", "passenger_missing_rows"):
        assert col in sql
    assert "limit 60" in sql and params == ["'2024-01'", "'2024-02'"]


def test_summary_query_bounded_and_partitioned():
    sql, params = queries_dq.build_summary_query(S, E)
    assert "mart_dq_summary" in sql and "pickup_month between ? and ?" in sql
    assert sql.rstrip().endswith("limit 50") and params == ["'2024-01'", "'2024-02'"]


@pytest.mark.parametrize("code", dq_reasons.REASON_CODES)
def test_examples_query_every_reason(code):
    sql, params = queries_dq.build_examples_query(S, E, code, None, None)
    assert "dq_suspect_trips" in sql and "pickup_month between ? and ?" in sql
    assert "reason_code = ?" in sql and "order by" in sql
    assert sql.rstrip().endswith("limit 200")
    assert params == ["'2024-01'", "'2024-02'", f"'{code}'"]
    assert code not in sql


@pytest.mark.parametrize("code,order", [
    ("SPEED_IMPLAUSIBLE", "avg_speed_mph desc"),
    ("DISTANCE_ABSURD", "trip_distance_miles desc"),
    ("ZERO_DISTANCE_PAID", "total_amount desc"),
    ("TIMESTAMP_SUSPECT", "trip_duration_min asc"),
    ("LONG_DURATION", "trip_duration_min desc"),
])
def test_examples_order(code, order):
    sql, _ = queries_dq.build_examples_query(S, E, code, None, None)
    assert f"order by {order}" in sql


def test_examples_filters_bound_as_params():
    sql, params = queries_dq.build_examples_query(S, E, "ZERO_TOTAL", "VeriFone", "Credit card")
    assert "vendor_name = ?" in sql and "payment_type_name = ?" in sql
    assert params[-2:] == ["'VeriFone'", "'Credit card'"]
    assert "VeriFone" not in sql and "Credit card" not in sql


@pytest.mark.parametrize("bad", ["x' or 1=1 --", "a;drop", "", "x" * 60])
def test_examples_rejects_bad_filter(bad):
    with pytest.raises(ValueError):
        queries_dq.build_examples_query(S, E, "ZERO_TOTAL", bad, None)
    with pytest.raises(ValueError):
        queries_dq.build_examples_query(S, E, "ZERO_TOTAL", None, bad)


def test_examples_rejects_bad_reason_and_range():
    with pytest.raises(ValueError):
        queries_dq.build_examples_query(S, E, "NOPE' --", None, None)
    with pytest.raises(ValueError):
        queries_dq.build_examples_query(S, date(2026, 1, 1), "ZERO_TOTAL", None, None)


@pytest.mark.parametrize("code", dq_reasons.REASON_CODES)
def test_distribution_query_aggregates_in_sql(code):
    sql, params = queries_dq.build_distribution_query(S, E, code)
    assert "dq_suspect_trips" in sql and "group by 1" in sql and "count(*)" in sql
    assert "pickup_month between ? and ?" in sql and "reason_code = ?" in sql
    assert sql.rstrip().endswith("limit 100") and params[-1] == f"'{code}'"


def test_distribution_axis_labels():
    assert "mph" in queries_dq.DIST_AXIS["SPEED_IMPLAUSIBLE"]
    assert set(queries_dq.DIST_AXIS) == set(dq_reasons.REASON_CODES)
