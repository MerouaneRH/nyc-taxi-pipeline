"""Pandas-side derived metrics. Pure functions."""

import pandas as pd

Z_THRESHOLD = 2.0
MIN_PER_WEEKDAY = 4


def ratio(df: pd.DataFrame, num: str, den: str, out: str, scale: float = 1.0) -> pd.DataFrame:
    """Add `out = scale * num / den`; NaN where the denominator is zero."""
    res = df.copy()
    res[out] = scale * res[num] / res[den].where(res[den] != 0)
    return res


def add_tip_pct(df: pd.DataFrame) -> pd.DataFrame:
    return ratio(df, "card_tip_revenue", "card_fare_revenue", "tip_pct", 100.0)


def week_over_week(daily: pd.DataFrame) -> pd.DataFrame:
    """Weekly (Mon-Sun) trips/revenue with % change; partial weeks are dropped."""
    if daily.empty:
        return pd.DataFrame()
    df = daily.assign(pickup_date=pd.to_datetime(daily["pickup_date"]))
    weekly = (
        df.groupby(df["pickup_date"].dt.to_period("W-SUN"))
        .agg(days=("pickup_date", "nunique"), trips=("trips", "sum"), revenue=("revenue", "sum"))
        .reset_index()
    )
    weekly = weekly[weekly["days"] == 7].reset_index(drop=True)
    weekly["week_start"] = weekly["pickup_date"].dt.start_time.dt.date
    weekly["trips_wow_pct"] = weekly["trips"].pct_change(fill_method=None) * 100
    weekly["revenue_wow_pct"] = weekly["revenue"].pct_change(fill_method=None) * 100
    return weekly.drop(columns=["pickup_date", "days"])


def flag_anomalies(daily: pd.DataFrame) -> pd.DataFrame:
    """Mark days whose revenue is > 2 std devs from the mean of the same weekday."""
    df = daily.copy()
    dow = pd.to_datetime(df["pickup_date"]).dt.dayofweek
    grp = df.groupby(dow)["revenue"]
    mean, std, n = grp.transform("mean"), grp.transform("std"), grp.transform("count")
    df["zscore"] = (df["revenue"] - mean) / std.where(std > 0)
    df["is_anomaly"] = (df["zscore"].abs() > Z_THRESHOLD) & (n >= MIN_PER_WEEKDAY)
    return df
