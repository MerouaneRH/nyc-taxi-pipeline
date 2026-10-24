from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest
import streamlit as st

from app import loaders
from app.queries import Filters

F = Filters(date(2024, 1, 5), date(2024, 1, 20))
PAIRS = [
    ("load_tips_by_hour", "pickup_hour"), ("load_tips_by_borough", "pickup_borough"),
    ("load_efficiency_by_hour", "pickup_hour"), ("load_efficiency_by_borough", "pickup_borough"),
]


@pytest.fixture(autouse=True)
def _fresh_cache():
    st.cache_data.clear()
    yield
    st.cache_data.clear()


def test_hourly_loaders_do_not_share_cache_entries():
    """Real st.cache_data, only Athena is faked: each loader must run its own SQL."""
    seen = []

    def fake(client, sql, params=None):
        seen.append(sql)
        return pd.DataFrame({"sql": [sql]})

    with patch.object(loaders, "run_query", fake), patch.object(loaders, "get_client"):
        results = {name: getattr(loaders, name)(F)["sql"][0] for name, _ in PAIRS}
    assert len(set(results.values())) == 4 and len(seen) == 4
    for name, col in PAIRS:
        assert f"select {col}," in results[name]
    assert "card_tip_revenue" in results["load_tips_by_hour"]
    assert "distance_miles_sum" in results["load_efficiency_by_borough"]
