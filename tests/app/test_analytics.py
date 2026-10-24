from datetime import date, timedelta

import pandas as pd
import pytest

from app import analytics


def _daily(days, fn):
    start = date(2024, 1, 1)  # a Monday
    return pd.DataFrame({
        "pickup_date": [start + timedelta(d) for d in range(days)],
        "trips": [fn(d)[0] for d in range(days)],
        "revenue": [fn(d)[1] for d in range(days)],
    })


def test_ratio_safe_divide():
    df = pd.DataFrame({"n": [1.0, 2.0], "d": [2.0, 0.0]})
    out = analytics.ratio(df, "n", "d", "r")
    assert out["r"].iloc[0] == 0.5 and pd.isna(out["r"].iloc[1])


def test_tip_pct():
    df = pd.DataFrame({"card_tip_revenue": [20.0], "card_fare_revenue": [100.0]})
    assert analytics.add_tip_pct(df)["tip_pct"].iloc[0] == pytest.approx(20.0)


def test_week_over_week_drops_partial_weeks():
    df = _daily(17, lambda d: (10 if d < 7 else 20, 100.0))  # wk1 full, wk2 full, wk3 partial(3d)
    wow = analytics.week_over_week(df)
    assert len(wow) == 2
    assert pd.isna(wow["trips_wow_pct"].iloc[0])
    assert wow["trips_wow_pct"].iloc[1] == pytest.approx(100.0)
    assert wow["revenue_wow_pct"].iloc[1] == pytest.approx(0.0)


def test_week_over_week_empty():
    assert analytics.week_over_week(_daily(3, lambda d: (1, 1.0))).empty


def test_anomalies_flag_outlier_same_weekday():
    # 10 weeks; revenue 100 +/- tiny noise, except one Monday = 1000.
    df = _daily(70, lambda d: (1, 100.0 + (d % 3) + (900 if d == 35 else 0)))
    out = analytics.flag_anomalies(df)
    flagged = out[out["is_anomaly"]]
    assert list(flagged["pickup_date"]) == [date(2024, 1, 1) + timedelta(35)]


def test_anomalies_ignore_constant_series_and_small_groups():
    flat = analytics.flag_anomalies(_daily(70, lambda d: (1, 100.0)))
    assert not flat["is_anomaly"].any()
    tiny = analytics.flag_anomalies(_daily(9, lambda d: (1, 1000.0 if d == 0 else 1.0)))
    assert not tiny["is_anomaly"].any()
