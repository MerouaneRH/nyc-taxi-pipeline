"""Athena data access for the dashboard. No UI code besides cache decorators.

Safety: every user-controlled value is validated, then bound through Athena
ExecutionParameters (`?`). SQL text is built only from constants and a
validated integer LIMIT. Every query has a LIMIT. Heavy reads hit the small
daily mart; only the heatmap touches fct_trips, always date-bounded.
"""

import os
import re
import time
from datetime import date

import boto3
import pandas as pd
import streamlit as st

WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "nyc-taxi-dbt")
REGION = os.environ.get("AWS_REGION", "eu-west-3")
DATABASE = os.environ.get("ATHENA_DATABASE", "nyc_taxi_dbt")
OUTPUT = os.environ.get("ATHENA_OUTPUT", "s3://merouane-nyc-taxi-raw-euw3-2026/athena-results/")
POLL_SECONDS = 0.5
MAX_POLLS = 240  # ~2 min
MAX_RANGE_DAYS = 400
MAX_LIMIT = 100
TTL = 3600
_BOROUGH_RE = re.compile(r"^[A-Za-z][A-Za-z /.-]{0,39}$")
_INT_TYPES = {"tinyint", "smallint", "integer", "bigint"}
_FLOAT_TYPES = {"float", "double", "decimal"}

BOROUGHS_SQL = (
    "select distinct pickup_borough as borough from mart_daily_zone_revenue "
    "where pickup_borough is not null order by 1 limit 50"
)
BOUNDS_SQL = (
    "select min(pickup_date) as min_date, max(pickup_date) as max_date "
    "from mart_daily_zone_revenue limit 1"
)


class QueryError(RuntimeError):
    """Athena query failed, was cancelled, or timed out."""


def validate_range(start: date, end: date) -> tuple[date, date]:
    if not isinstance(start, date) or not isinstance(end, date):
        raise ValueError("start and end must be dates")  # noqa: TRY004
    if start > end:
        raise ValueError("start must be <= end")
    if (end - start).days > MAX_RANGE_DAYS:
        raise ValueError(f"range exceeds {MAX_RANGE_DAYS} days")
    return start, end


def validate_borough(borough: str | None) -> str | None:
    if borough is None or borough == "All":
        return None
    if not isinstance(borough, str) or not _BOROUGH_RE.match(borough):
        raise ValueError("invalid borough")
    return borough


def validate_limit(limit: int) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be an int in 1..{MAX_LIMIT}")
    return limit


def _filter(start: date, end: date, borough: str | None) -> tuple[str, list[str]]:
    start, end = validate_range(start, end)
    borough = validate_borough(borough)
    where = "pickup_date between cast(? as date) and cast(? as date)"
    params = [f"'{start.isoformat()}'", f"'{end.isoformat()}'"]
    if borough:
        where += " and pickup_borough = ?"
        params.append(f"'{borough}'")
    return where, params


def build_daily_query(start: date, end: date, borough: str | None) -> tuple[str, list[str]]:
    where, params = _filter(start, end, borough)
    sql = (
        "select pickup_date, sum(trips) as trips, sum(total_revenue) as revenue, "
        "sum(fare_revenue) as fare_revenue, sum(tip_revenue) as tip_revenue "
        f"from mart_daily_zone_revenue where {where} group by 1 order by 1 limit 1000"
    )
    return sql, params


def build_zones_query(
    start: date, end: date, borough: str | None, top_n: int
) -> tuple[str, list[str]]:
    where, params = _filter(start, end, borough)
    n = validate_limit(top_n)
    sql = (
        "select pickup_zone, pickup_borough, sum(trips) as trips, sum(total_revenue) as revenue "
        f"from mart_daily_zone_revenue where {where} and pickup_zone is not null "
        f"group by 1, 2 order by 4 desc limit {n}"
    )
    return sql, params


def build_heatmap_query(start: date, end: date, borough: str | None) -> tuple[str, list[str]]:
    where, params = _filter(start, end, borough)
    sql = (
        "select pickup_dow, pickup_hour, count(*) as trips from fct_trips "
        f"where {where} group by 1, 2 order by 1, 2 limit 200"
    )
    return sql, params


def _convert(series: pd.Series, athena_type: str) -> pd.Series:
    if athena_type in _INT_TYPES:
        return pd.to_numeric(series).astype("int64")
    if athena_type in _FLOAT_TYPES:
        return pd.to_numeric(series).astype("float64")
    if athena_type == "date":
        return pd.to_datetime(series).dt.date
    return series


def _wait(client, qid: str) -> dict:
    for _ in range(MAX_POLLS):
        qe = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = qe["Status"]["State"]
        if state == "SUCCEEDED":
            return qe
        if state in ("FAILED", "CANCELLED"):
            raise QueryError(f"{state}: {qe['Status'].get('StateChangeReason', '')}")
        time.sleep(POLL_SECONDS)
    client.stop_query_execution(QueryExecutionId=qid)
    raise QueryError("query timed out")


def _fetch(client, qid: str) -> pd.DataFrame:
    rows, cols = [], []
    for page in client.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        rs = page["ResultSet"]
        cols = rs["ResultSetMetadata"]["ColumnInfo"]
        rows += [[c.get("VarCharValue") for c in r["Data"]] for r in rs["Rows"]]
    names = [c["Label"] for c in cols]
    df = pd.DataFrame(rows[1:], columns=names)  # first row of page 1 is the header
    for c in cols:
        df[c["Label"]] = _convert(df[c["Label"]], c["Type"])
    return df


def run_query(client, sql: str, params: list[str] | None = None) -> pd.DataFrame:
    """Run one query and return a typed DataFrame; scanned bytes in df.attrs."""
    kwargs = {
        "QueryString": sql,
        "QueryExecutionContext": {"Database": DATABASE},
        "WorkGroup": WORKGROUP,
        "ResultConfiguration": {"OutputLocation": OUTPUT},
    }
    if params:
        kwargs["ExecutionParameters"] = params
    qid = client.start_query_execution(**kwargs)["QueryExecutionId"]
    qe = _wait(client, qid)
    df = _fetch(client, qid)
    df.attrs["scanned_bytes"] = qe.get("Statistics", {}).get("DataScannedInBytes", 0)
    return df


def compute_kpis(daily: pd.DataFrame) -> dict:
    trips = int(daily["trips"].sum())
    revenue = float(daily["revenue"].sum())
    fare = float(daily["fare_revenue"].sum())
    tip = float(daily["tip_revenue"].sum())
    return {"trips": trips, "revenue": revenue, "avg_tip_pct": 100 * tip / fare if fare else 0.0}


@st.cache_resource
def get_client():
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE", "nyc-taxi"), region_name=REGION)
    return session.client("athena")


@st.cache_data(ttl=TTL, max_entries=4, show_spinner="Loading boroughs...")
def load_boroughs() -> list[str]:
    return run_query(get_client(), BOROUGHS_SQL)["borough"].tolist()


@st.cache_data(ttl=TTL, max_entries=4, show_spinner="Loading date bounds...")
def load_bounds() -> tuple[date, date]:
    row = run_query(get_client(), BOUNDS_SQL).iloc[0]
    return row["min_date"], row["max_date"]


@st.cache_data(ttl=TTL, max_entries=32, show_spinner="Querying daily revenue...")
def load_daily(start: date, end: date, borough: str | None) -> pd.DataFrame:
    return run_query(get_client(), *build_daily_query(start, end, borough))


@st.cache_data(ttl=TTL, max_entries=32, show_spinner="Querying top zones...")
def load_zones(start: date, end: date, borough: str | None, top_n: int = 15) -> pd.DataFrame:
    return run_query(get_client(), *build_zones_query(start, end, borough, top_n))


@st.cache_data(ttl=TTL, max_entries=16, show_spinner="Querying hourly demand...")
def load_heatmap(start: date, end: date, borough: str | None) -> pd.DataFrame:
    return run_query(get_client(), *build_heatmap_query(start, end, borough))
