import sys
from datetime import timedelta
from pathlib import Path

import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import data, freshness, loaders
from app.queries import DAY_TYPES, Filters
from app.views import airports, efficiency, overview, quality, routes, tips, trends
from app.views.common import safe_render

VIEWS = ["Overview", "Tips", "Airports", "Routes", "Efficiency", "Trends", "Data quality"]
HOURLY_VIEWS = {"Tips", "Efficiency"}

st.set_page_config(page_title="NYC taxi", page_icon=":material/local_taxi:", layout="wide")
st.title("NYC taxi dashboard")

freshness.refresh_if_new()

try:
    boroughs = data.load_boroughs()
    bounds = data.load_bounds()
except (BotoCoreError, ClientError, data.QueryError) as exc:
    st.error(data.friendly_error(exc), icon=":material/error:")
    st.stop()

if bounds is None:
    st.info("No data in the mart yet. Run the pipeline first.")
    st.stop()
min_d, max_d = bounds

# Only the selected view runs queries; deselecting falls back to Overview.
view = st.segmented_control("View", VIEWS, default="Overview", label_visibility="collapsed") \
    or "Overview"


def hourly_filters() -> dict:
    """Sidebar widgets for the hourly mart; options are queried only for those views."""
    try:
        payments, vendors = loaders.load_payments(), loaders.load_vendors()
    except (BotoCoreError, ClientError, data.QueryError) as exc:
        st.error(data.friendly_error(exc), icon=":material/error:")
        st.stop()
    lo, hi = st.slider("Pickup hour", 0, 23, (0, 23))
    return {
        "payments": tuple(st.multiselect("Payment type", payments)),
        "vendors": tuple(st.multiselect("Vendor", vendors)),
        "hour_min": lo, "hour_max": hi,
        # pickup_dow follows Trino day_of_week(): 1 = Monday ... 7 = Sunday
        "day_type": st.segmented_control("Day type", DAY_TYPES, default="All") or "All",
    }


with st.sidebar:
    freshness.watcher()
    st.header("Filters")
    borough = st.selectbox("Borough", ["All", *boroughs])
    default_start = max(min_d, max_d - timedelta(days=29))
    picked = st.date_input("Pickup dates", (default_start, max_d), min_value=min_d, max_value=max_d)
    top_n = st.slider("Top zones", 5, 30, 15) if view == "Overview" else 15
    extra = hourly_filters() if view in HOURLY_VIEWS else {}

if len(picked) != 2:
    st.info("Pick an end date.")
    st.stop()

filters = Filters(*picked, None if borough == "All" else borough, **extra)

renderers = {
    "Tips": (tips.render, filters),
    "Airports": (airports.render, filters.start, filters.end),
    "Routes": (routes.render, filters),
    "Efficiency": (efficiency.render, filters),
    "Trends": (trends.render, filters, min_d),
    "Data quality": (quality.render, filters),
}
safe_render(*renderers.get(view, (overview.render, filters, top_n)))
