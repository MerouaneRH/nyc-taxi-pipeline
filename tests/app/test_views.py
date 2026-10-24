from contextlib import ExitStack
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from streamlit.testing.v1 import AppTest

from app import data, loaders

MAIN = Path(__file__).resolve().parents[2] / "app" / "main.py"
ARN = "arn:aws:iam::123456789012:role/secret"
DATES = pd.date_range("2024-01-01", periods=70).date


def _daily():
    return pd.DataFrame({"pickup_date": DATES, "trips": 100, "revenue": 1000.0,
                         "fare_revenue": 800.0, "tip_revenue": 100.0})


def _zones():
    return pd.DataFrame({"pickup_zone": ["A"], "pickup_borough": ["Queens"],
                         "trips": [5], "revenue": [50.0]})


def _hour(by):
    key = "pickup_hour" if by == "hour" else "pickup_borough"
    vals = [8, 9] if by == "hour" else ["Queens", "Bronx"]
    return pd.DataFrame({key: vals, "card_tip_revenue": 10.0, "card_fare_revenue": 50.0,
                         "total_revenue": 100.0, "distance_miles_sum": 20.0,
                         "duration_min_sum": 60.0, "trips": 10})


def _attr(df, mb=1234567):
    df.attrs["scanned_bytes"] = mb
    return df


STUBS = {
    "load_boroughs": ["Queens"], "load_bounds": (date(2024, 1, 1), date(2024, 3, 10)),
    "load_daily": _daily(), "load_zones": _zones(),
}
LSTUBS = {
    "load_payments": ["Credit card", "Cash"], "load_vendors": ["VeriFone"],
    "load_tips_by_hour": _attr(_hour("hour")), "load_tips_by_borough": _attr(_hour("b")),
    "load_efficiency_by_hour": _attr(_hour("hour")),
    "load_efficiency_by_borough": _attr(_hour("b")),
    "load_airports": _attr(pd.DataFrame({"grp": ["JFK Airport", "Other"], "trips": [10, 90],
                                         "revenue": [500.0, 900.0]})),
    "load_top_routes": _attr(pd.DataFrame({
        "pickup_zone": ["A"], "dropoff_zone": ["B"], "pickup_borough": ["Queens"],
        "dropoff_borough": ["Bronx"], "trips": [5], "total_revenue": [50.0]})),
    "load_borough_matrix": _attr(pd.DataFrame({
        "pickup_borough": ["Queens"], "dropoff_borough": ["Bronx"], "trips": [5]})),
    "load_trend_daily": _attr(_daily()),
    "load_quality": _attr(pd.DataFrame({
        "month": ["2024-01"], "raw_rows": [100], "staged_rows": [98], "dropped_rows": [2],
        "dropped_pct": [2.0], "implausible_speed_rows": [3], "built_at": ["2024-02-01"]})),
}


def _run(view=None, overrides=None):
    with ExitStack() as stack:
        for mod, stubs in ((data, STUBS), (loaders, LSTUBS)):
            for name, val in {**stubs, **(overrides or {})}.items():
                if not hasattr(mod, name):
                    continue
                kw = {"side_effect": val} if isinstance(val, Exception) else {"return_value": val}
                stack.enter_context(patch.object(mod, name, **kw))
        at = AppTest.from_file(str(MAIN), default_timeout=30).run()
        if view:
            at.segmented_control[0].set_value(view).run()
        return at


def _spy_all():
    return {n: n for n in LSTUBS}


VIEWS = ["Overview", "Tips", "Airports", "Routes", "Efficiency", "Trends", "Data quality"]


@pytest.mark.parametrize("view", VIEWS)
def test_view_renders_without_exception(view):
    at = _run(view)
    assert not at.exception, at.exception
    assert not at.error


@pytest.mark.parametrize("view", VIEWS[1:])
def test_non_overview_view_shows_scanned_mb(view):
    at = _run(view)
    assert any("scanned" in c.value.lower() and "MB" in c.value for c in at.caption)


def test_tips_note_cash_not_recorded():
    at = _run("Tips")
    assert any("cash" in (m.value or "").lower() for m in [*at.caption, *at.info, *at.markdown])


def test_lazy_only_selected_view_queries():
    with ExitStack() as stack:
        mocks = {n: stack.enter_context(patch.object(loaders, n, return_value=v))
                 for n, v in LSTUBS.items()}
        for n, v in STUBS.items():
            stack.enter_context(patch.object(data, n, return_value=v))
        at = AppTest.from_file(str(MAIN), default_timeout=30).run()
        assert not at.exception
        assert not any(m.called for m in mocks.values())  # Overview touches no new loader
        at.segmented_control[0].set_value("Airports").run()
        assert mocks["load_airports"].called
        assert not mocks["load_tips_by_hour"].called and not mocks["load_top_routes"].called


def test_hourly_filters_present_for_tips():
    at = _run("Tips")
    labels = [w.label for w in at.sidebar.multiselect]
    assert "Payment type" in labels and "Vendor" in labels
    assert len(at.sidebar.slider) >= 1


@pytest.mark.parametrize("view,loader", [("Tips", "load_tips_by_hour"), ("Routes", "load_top_routes"),
                                          ("Data quality", "load_quality"),
                                          ("Trends", "load_trend_daily")])
def test_boto_error_is_friendly(view, loader):
    err = ClientError({"Error": {"Code": "AccessDenied", "Message": ARN}}, "StartQueryExecution")
    at = _run(view, {loader: err})
    assert not at.exception
    assert len(at.error) == 1
    assert ARN not in at.error[0].value and "AccessDenied" not in at.error[0].value


def test_trends_marks_anomaly():
    d = _daily()
    d.loc[35, "revenue"] = 100000.0
    at = _run("Trends", {"load_trend_daily": _attr(d)})
    assert not at.exception
    assert len(at.dataframe) == 1
    assert list(at.dataframe[0].value["pickup_date"]) == [DATES[35]]
