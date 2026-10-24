import altair as alt
import streamlit as st

from app import loaders
from app.queries import Filters
from app.views.common import fetch, scanned_caption

TOP_N = 20


def render(f: Filters) -> None:
    st.caption("Route data is monthly: the date range is widened to whole months.")
    top = fetch(loaders.load_top_routes, f.start, f.end, f.borough, TOP_N)
    matrix = fetch(loaders.load_borough_matrix, f.start, f.end, f.borough)
    if top is None or matrix is None:
        return
    if top.empty:
        st.warning("No trips for this selection.")
        return
    st.subheader(f"Top {TOP_N} pickup to dropoff pairs")
    st.dataframe(top, hide_index=True, alt="Top pickup to dropoff zone pairs by trips")
    st.subheader("Borough to borough trips")
    chart = (
        alt.Chart(matrix)
        .mark_rect()
        .encode(
            x=alt.X("dropoff_borough:N", title="Dropoff borough"),
            y=alt.Y("pickup_borough:N", title="Pickup borough"),
            color=alt.Color("trips:Q", title="Trips"),
            tooltip=["pickup_borough", "dropoff_borough", "trips"],
        )
    )
    st.altair_chart(chart, alt="Matrix of trips from pickup borough to dropoff borough")
    scanned_caption(top, "Routes query")
    scanned_caption(matrix, "Matrix query")
