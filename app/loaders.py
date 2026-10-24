"""Cached Athena loaders for the insight views (the Overview loaders live in data.py)."""

from datetime import date

import pandas as pd
import streamlit as st

from app import queries, queries_dq
from app.data import TTL, build_daily_query, get_client, run_query
from app.queries import Filters

# One explicit function per query: st.cache_data keys on the function's qualified
# name plus its arguments, so a shared factory-made inner function would collide.
_SPIN = "Querying hourly mart..."


@st.cache_data(ttl=TTL, max_entries=16, show_spinner=_SPIN)
def load_tips_by_hour(f: Filters) -> pd.DataFrame:
    return run_query(get_client(), *queries.build_tips_by_hour(f))


@st.cache_data(ttl=TTL, max_entries=16, show_spinner=_SPIN)
def load_tips_by_borough(f: Filters) -> pd.DataFrame:
    return run_query(get_client(), *queries.build_tips_by_borough(f))


@st.cache_data(ttl=TTL, max_entries=16, show_spinner=_SPIN)
def load_efficiency_by_hour(f: Filters) -> pd.DataFrame:
    return run_query(get_client(), *queries.build_efficiency_by_hour(f))


@st.cache_data(ttl=TTL, max_entries=16, show_spinner=_SPIN)
def load_efficiency_by_borough(f: Filters) -> pd.DataFrame:
    return run_query(get_client(), *queries.build_efficiency_by_borough(f))


@st.cache_data(ttl=TTL, max_entries=4, show_spinner="Loading payment types...")
def load_payments() -> list[str]:
    return run_query(get_client(), queries.PAYMENTS_SQL)["value"].tolist()


@st.cache_data(ttl=TTL, max_entries=4, show_spinner="Loading vendors...")
def load_vendors() -> list[str]:
    return run_query(get_client(), queries.VENDORS_SQL)["value"].tolist()


@st.cache_data(ttl=TTL, max_entries=16, show_spinner="Querying airports...")
def load_airports(start: date, end: date) -> pd.DataFrame:
    return run_query(get_client(), *queries.build_airports_query(start, end))


@st.cache_data(ttl=TTL, max_entries=16, show_spinner="Querying routes...")
def load_top_routes(start: date, end: date, borough: str | None, top_n: int = 20) -> pd.DataFrame:
    return run_query(get_client(), *queries.build_top_routes_query(start, end, borough, top_n))


@st.cache_data(ttl=TTL, max_entries=16, show_spinner="Querying borough matrix...")
def load_borough_matrix(start: date, end: date, borough: str | None) -> pd.DataFrame:
    return run_query(get_client(), *queries.build_borough_matrix_query(start, end, borough))


@st.cache_data(ttl=TTL, max_entries=16, show_spinner="Querying data quality...")
def load_quality(start: date, end: date) -> pd.DataFrame:
    return run_query(get_client(), *queries.build_quality_query(start, end))


@st.cache_data(ttl=TTL, max_entries=16, show_spinner="Querying daily trend...")
def load_trend_daily(start: date, end: date, borough: str | None) -> pd.DataFrame:
    return run_query(get_client(), *build_daily_query(start, end, borough))


@st.cache_data(ttl=TTL, max_entries=16, show_spinner="Querying suspect-trip summary...")
def load_dq_summary(start: date, end: date) -> pd.DataFrame:
    return run_query(get_client(), *queries_dq.build_summary_query(start, end))


@st.cache_data(ttl=TTL, max_entries=32, show_spinner="Loading example suspect trips...")
def load_dq_examples(
    start: date, end: date, reason: str, vendor: str | None, payment: str | None
) -> pd.DataFrame:
    return run_query(get_client(), *queries_dq.build_examples_query(start, end, reason, vendor, payment))


@st.cache_data(ttl=TTL, max_entries=32, show_spinner="Building distribution...")
def load_dq_distribution(start: date, end: date, reason: str) -> pd.DataFrame:
    return run_query(get_client(), *queries_dq.build_distribution_query(start, end, reason))
