"""Notice when the pipeline has produced new data, so cached results never go stale."""

import logging

import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError

from app import data

log = logging.getLogger(__name__)

# One row, a few bytes: the build time of the newest quality table plus the latest month.
VERSION_SQL = (
    "select cast(max(built_at) as varchar) as built_at, cast(max(month) as varchar) as latest_month "
    "from mart_month_quality limit 1"
)
CHECK_EVERY = "60s"


def query_version() -> str | None:
    df = data.run_query(data.get_client(), VERSION_SQL)
    if df.empty or df.iloc[0]["built_at"] is None:
        return None
    return f"{df.iloc[0]['built_at']}|{df.iloc[0]['latest_month']}"


@st.cache_data(ttl=20, show_spinner=False)
def _fetch_version() -> str | None:
    try:
        return query_version()
    except (BotoCoreError, ClientError, data.QueryError):
        log.exception("data version lookup failed")
        return None


@st.cache_resource
def _store() -> dict:
    return {"v": None}


def changed(store: dict, current: str | None) -> bool:
    """True when `current` differs from the last seen version. The first sighting never counts."""
    if current is None:
        return False
    seen = store["v"]
    store["v"] = current
    return seen is not None and seen != current


def refresh_if_new() -> str | None:
    """Drop every cached query result when a newer build is detected. Returns the version."""
    version = _fetch_version()
    if changed(_store(), version):
        log.info("new data detected (%s): clearing caches", version)
        st.cache_data.clear()
    return version


@st.fragment(run_every=CHECK_EVERY)
def watcher() -> None:
    """Sidebar fragment: re-checks on a timer and reruns the app when new data lands."""
    before = _store()["v"]
    version = refresh_if_new()
    if before is not None and version is not None and version != before:
        st.rerun()
    if version:
        built, _, month = version.partition("|")
        st.caption(f"Data built {built[:16]} UTC. Latest month: {month}.")
