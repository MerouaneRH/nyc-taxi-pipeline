import streamlit as st

from app import loaders
from app.queries import AIRPORTS
from app.views.common import fetch, scanned_caption


def render(start, end) -> None:
    df = fetch(loaders.load_airports, start, end)
    if df is None:
        return
    total = int(df["trips"].sum())
    if total == 0:
        st.warning("No trips for this selection.")
        return
    st.caption("All boroughs; the borough filter does not apply here.")
    cols = st.columns(len(AIRPORTS))
    for col, zone in zip(cols, AIRPORTS, strict=True):
        row = df[df["grp"] == zone]
        trips = int(row["trips"].sum())
        revenue = float(row["revenue"].sum())
        with col, st.container(border=True):
            st.subheader(zone)
            st.metric("Trips", f"{trips:,}")
            st.metric("Revenue", f"${revenue:,.0f}")
            st.metric("Avg fare", f"${revenue / trips:,.2f}" if trips else "n/a")
            st.metric("Share of all trips", f"{100 * trips / total:.2f}%")
    scanned_caption(df, "Query")
