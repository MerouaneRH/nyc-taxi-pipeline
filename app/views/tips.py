import streamlit as st

from app import analytics, loaders
from app.queries import Filters
from app.views.common import fetch, hour_line_chart, scanned_caption


def render(f: Filters) -> None:
    st.info(
        "Cash tips are not recorded in the TLC data, so tip % covers card payments only: "
        "card tips divided by card fares.",
        icon=":material/info:",
    )
    by_hour = fetch(loaders.load_tips_by_hour, f)
    by_borough = fetch(loaders.load_tips_by_borough, f)
    if by_hour is None or by_borough is None:
        return
    if by_hour.empty:
        st.warning("No trips for this selection.")
        return
    if by_hour["card_fare_revenue"].fillna(0).sum() == 0:
        st.info("No card payments in this selection. Tips are only recorded for card payments.",
                icon=":material/info:")
        scanned_caption(by_hour, "Hourly query")
        return
    st.subheader("Tip % by hour")
    st.altair_chart(hour_line_chart(analytics.add_tip_pct(by_hour), "tip_pct", "Card tip % by hour"),
                    alt="Card tip percentage by pickup hour")
    st.subheader("Tip % by borough")
    st.bar_chart(analytics.add_tip_pct(by_borough), x="pickup_borough", y="tip_pct",
                 alt="Card tip percentage by pickup borough")
    scanned_caption(by_hour, "Hourly query")
    scanned_caption(by_borough, "Borough query")
