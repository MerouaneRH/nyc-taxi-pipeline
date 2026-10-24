from datetime import date

import pytest

from app import queries
from app.queries import Filters


def _f(**kw):
    base = {"start": date(2024, 1, 5), "end": date(2024, 2, 10), "borough": None,
            "payments": (), "vendors": (), "hour_min": 0, "hour_max": 23, "day_type": "All"}
    base.update(kw)
    return Filters(**base)


BUILDERS = [
    lambda f: queries.build_tips_by_hour(f),
    lambda f: queries.build_tips_by_borough(f),
    lambda f: queries.build_efficiency_by_hour(f),
    lambda f: queries.build_efficiency_by_borough(f),
]


@pytest.mark.parametrize("build", BUILDERS)
def test_hourly_queries_partition_limit_and_params(build):
    sql, params = build(_f(borough="Queens", payments=("Credit card",), vendors=("VeriFone",),
                           hour_min=6, hour_max=9, day_type="Weekday"))
    assert "mart_hourly_borough" in sql and "limit" in sql
    assert "pickup_month between ? and ?" in sql
    assert params[:2] == ["'2024-01'", "'2024-02'"]
    for value in ("Queens", "Credit card", "VeriFone"):
        assert value not in sql and f"'{value}'" in params
    assert sql.count("?") == len(params)
    assert "pickup_dow between 1 and 5" in sql
    assert "pickup_hour between 6 and 9" in sql


def test_weekend_day_type_and_all():
    assert "pickup_dow between 6 and 7" in queries.build_tips_by_hour(_f(day_type="Weekend"))[0]
    assert "pickup_dow" not in queries.build_tips_by_hour(_f())[0]


def test_multi_value_placeholders():
    sql, params = queries.build_tips_by_hour(_f(payments=("a", "b", "c")))
    assert "payment_type_name in (?, ?, ?)" in sql
    assert params[-3:] == ["'a'", "'b'", "'c'"]


@pytest.mark.parametrize("kw", [
    {"payments": ("x'; drop table t;--",)},
    {"vendors": ("a" * 80,)},
    {"payments": tuple(f"p{i}" for i in range(30))},
    {"day_type": "Funday"},
    {"hour_min": -1}, {"hour_max": 24}, {"hour_min": 10, "hour_max": 5},
    {"hour_min": "1; drop"}, {"borough": "Queens'; --"},
])
def test_filters_reject_bad_input(kw):
    with pytest.raises(ValueError):
        queries.build_tips_by_hour(_f(**kw))


def test_airports_query_constant_zones_and_params():
    sql, params = queries.build_airports_query(date(2024, 1, 1), date(2024, 1, 31))
    assert "mart_daily_zone_revenue" in sql and "limit" in sql
    assert "JFK Airport" in sql and params == ["'2024-01-01'", "'2024-01-31'"]


def test_top_routes_and_matrix():
    sql, params = queries.build_top_routes_query(date(2024, 1, 5), date(2024, 2, 10), "Queens", 20)
    assert "mart_od_pairs" in sql and sql.rstrip().endswith("limit 20")
    assert params == ["'2024-01'", "'2024-02'", "'Queens'"]
    sql, params = queries.build_borough_matrix_query(date(2024, 1, 5), date(2024, 2, 10), None)
    assert "limit" in sql and params == ["'2024-01'", "'2024-02'"]
    with pytest.raises(ValueError):
        queries.build_top_routes_query(date(2024, 1, 1), date(2024, 1, 2), None, 10**6)


def test_quality_query():
    sql, params = queries.build_quality_query(date(2024, 1, 5), date(2024, 3, 10))
    assert "mart_month_quality" in sql and "limit" in sql
    assert "implausible_speed_rows" in sql and params == ["'2024-01'", "'2024-03'"]


def test_options_queries_bounded():
    for sql in (queries.PAYMENTS_SQL, queries.VENDORS_SQL):
        assert "mart_hourly_borough" in sql and "limit" in sql
