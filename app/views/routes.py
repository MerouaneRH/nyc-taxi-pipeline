import altair as alt
import streamlit as st

from app import loaders
from app.queries import Filters
from app.views.common import fetch, scanned_caption

TOP_N = 20


def borough_matrix_chart(matrix) -> alt.Chart:
    return (
        alt.Chart(matrix)
        .mark_rect()
        .encode(
            x=alt.X("dropoff_borough:N", title="Dropoff borough"),
            y=alt.Y("pickup_borough:N", title="Pickup borough"),
            color=alt.Color("trips:Q", title="Trips", scale=alt.Scale(type="log")),
            tooltip=["pickup_borough", "dropoff_borough", "trips"],
        )
    )


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
    st.caption("Trips with an unknown zone (N/A) are left out.")
    st.dataframe(
        top, hide_index=True, alt="Top pickup to dropoff zone pairs by trips",
        column_config={
            "trips": st.column_config.NumberColumn("Trips", format="localized"),
            "total_revenue": st.column_config.NumberColumn("Revenue", format="dollar"),
        },
    )
    st.subheader("Borough to borough trips")
    st.caption("Color scale is logarithmic so small flows stay visible next to Manhattan.")
    st.altair_chart(borough_matrix_chart(matrix),
                    alt="Matrix of trips from pickup borough to dropoff borough")
    scanned_caption(top, "Routes query")
    scanned_caption(matrix, "Matrix query")
