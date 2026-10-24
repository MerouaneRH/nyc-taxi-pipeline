import altair as alt
import streamlit as st

from app import data
from app.queries import Filters
from app.views.common import fetch

DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]  # pickup_dow 1 = Monday


def render(f: Filters, top_n: int) -> None:
    daily = fetch(data.load_daily, f.start, f.end, f.borough)
    zones = fetch(data.load_zones, f.start, f.end, f.borough, top_n)
    if daily is None or zones is None:
        return
    if daily.empty:
        st.warning("No trips for this selection.")
        return
    kpi = data.compute_kpis(daily)
    c1, c2, c3 = st.columns(3)
    c1.metric("Trips", f"{kpi['trips']:,}", border=True)
    c2.metric("Revenue", f"${kpi['revenue']:,.0f}", border=True)
    c3.metric("Avg tip (% of fare)", f"{kpi['avg_tip_pct']:.1f}%", border=True)
    st.subheader("Daily revenue")
    st.line_chart(daily, x="pickup_date", y="revenue", alt="Daily revenue line chart")
    st.subheader("Top pickup zones by revenue")
    st.bar_chart(zones, x="pickup_zone", y="revenue", horizontal=True, sort="-revenue",
                 alt="Top pickup zones by revenue")
    _heatmap(f)


def _heatmap(f: Filters) -> None:
    st.subheader("Trips by hour and weekday")
    if not st.toggle("Load heatmap (scans fct_trips)", value=True):
        return
    heat = fetch(data.load_heatmap, f.start, f.end, f.borough)
    if heat is None:
        return
    heat["weekday"] = heat["pickup_dow"].map(lambda i: DOW[i - 1])
    chart = (
        alt.Chart(heat)
        .mark_rect()
        .encode(
            x=alt.X("pickup_hour:O", title="Hour of day"),
            y=alt.Y("weekday:N", sort=DOW, title=None),
            color=alt.Color("trips:Q", title="Trips"),
            tooltip=["weekday", "pickup_hour", "trips"],
        )
    )
    st.altair_chart(chart, alt="Heatmap of trips by hour of day and weekday")
    st.caption(f"Heatmap scanned {heat.attrs.get('scanned_bytes', 0) / 1e6:,.1f} MB.")
