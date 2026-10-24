import pytest
import streamlit as st

from app import freshness


@pytest.fixture(autouse=True)
def _clean_cache():
    st.cache_data.clear()
    yield
    st.cache_data.clear()


def test_changed_false_on_first_sight_then_only_on_new_version():
    store = {"v": None}
    assert freshness.changed(store, "a") is False and store["v"] == "a"
    assert freshness.changed(store, "a") is False
    assert freshness.changed(store, "b") is True and store["v"] == "b"


def test_changed_ignores_failed_lookup():
    store = {"v": "a"}
    assert freshness.changed(store, None) is False and store["v"] == "a"


def test_new_version_clears_data_caches(monkeypatch):
    calls = []

    @st.cache_data
    def cached(x):
        calls.append(x)
        return x

    cached(1)
    cached(1)
    assert calls == [1]
    versions = iter(["v1", "v2"])
    monkeypatch.setattr(freshness, "query_version", lambda: next(versions))
    freshness.refresh_if_new()          # first sight: remembered, nothing cleared
    cached(1)
    assert calls == [1]
    st.cache_data.clear()               # drop the cached version lookup so the next call re-queries
    freshness.refresh_if_new()          # v2 differs from v1: caches must be cleared
    cached(1)
    assert calls == [1, 1]


def test_version_sql_is_tiny_and_bounded():
    assert "mart_month_quality" in freshness.VERSION_SQL and "limit" in freshness.VERSION_SQL.lower()
