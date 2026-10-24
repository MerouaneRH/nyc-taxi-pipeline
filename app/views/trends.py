from datetime import timedelta

import altair as alt
import streamlit as st

from app import analytics, loaders
from app.queries import Filters
from app.views.common import fetch, scanned_caption

HISTORY_DAYS = 111  # 16 weeks: enough same-weekday samples for the anomaly rule


def render(f: Filters, min_date) -> None:
    start = max(min_date, f.end - timedelta(days=HISTORY_DAYS))
    st.caption(f"Uses up to {HISTORY_DAYS + 1} days ending {f.end}, regardless of the start date.")
    daily = fetch(loaders.load_trend_daily, start, f.end, f.borough)
    if daily is None:
        return
    if daily.empty:
        st.warning("No trips for this selection.")
        return
    wow = analytics.week_over_week(daily)
    st.subheader("Week over week change")
    if wow.empty:
        st.info("Need at least one full Monday-to-Sunday week.")
    else:
        st.bar_chart(wow, x="week_start", y=["trips_wow_pct", "revenue_wow_pct"],
                     alt="Week over week % change in trips and revenue")
    flagged = analytics.flag_anomalies(daily)
    st.subheader("Anomaly days")
    st.caption("Anomaly: daily revenue more than 2 standard deviations from the mean of the "
               "same weekday. Marked in red.")
    base = alt.Chart(flagged).encode(x="pickup_date:T", y="revenue:Q")
    points = base.transform_filter("datum.is_anomaly").mark_point(color="red", size=90, filled=True)
    st.altair_chart(base.mark_line() + points, alt="Daily revenue with anomaly days marked")
    hits = flagged[flagged["is_anomaly"]]
    if hits.empty:
        st.caption("No anomaly days in this window.")
    else:
        st.dataframe(hits[["pickup_date", "revenue", "zscore"]], hide_index=True,
                     alt="Anomaly days with revenue and z-score")
    scanned_caption(daily, "Query")
