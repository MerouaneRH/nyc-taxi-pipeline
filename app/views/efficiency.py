import streamlit as st

from app import analytics, loaders
from app.queries import Filters
from app.views.common import fetch, hour_line_chart, scanned_caption


def _with_ratios(df):
    df = analytics.ratio(df, "total_revenue", "distance_miles_sum", "revenue_per_mile")
    return analytics.ratio(df, "total_revenue", "duration_min_sum", "revenue_per_min")


def render(f: Filters) -> None:
    by_hour = fetch(loaders.load_efficiency_by_hour, f)
    by_borough = fetch(loaders.load_efficiency_by_borough, f)
    if by_hour is None or by_borough is None:
        return
    if by_hour.empty:
        st.warning("No trips for this selection.")
        return
    hour, borough = _with_ratios(by_hour), _with_ratios(by_borough)
    for col, label in (("revenue_per_mile", "Revenue per mile"),
                       ("revenue_per_min", "Revenue per minute")):
        st.subheader(f"{label} by hour")
        st.altair_chart(hour_line_chart(hour, col, f"{label} by hour"),
                        alt=f"{label} by pickup hour")
        st.subheader(f"{label} by borough")
        st.bar_chart(borough, x="pickup_borough", y=col, alt=f"{label} by pickup borough")
    scanned_caption(by_hour, "Hourly query")
    scanned_caption(by_borough, "Borough query")
