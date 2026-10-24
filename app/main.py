import sys
from datetime import timedelta
from pathlib import Path

import altair as alt
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import data

DOW = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

st.set_page_config(page_title="NYC taxi", page_icon=":material/local_taxi:", layout="wide")
st.title("NYC taxi dashboard")

try:
    boroughs = data.load_boroughs()
    min_d, max_d = data.load_bounds()
except (BotoCoreError, ClientError, data.QueryError) as exc:
    st.error(f"Cannot reach Athena: {exc}", icon=":material/error:")
    st.stop()

with st.sidebar:
    st.header("Filters")
    borough = st.selectbox("Borough", ["All", *boroughs])
    default_start = max(min_d, max_d - timedelta(days=29))
    picked = st.date_input("Pickup dates", (default_start, max_d), min_value=min_d, max_value=max_d)
    top_n = st.slider("Top zones", 5, 30, 15)

if len(picked) != 2:
    st.info("Pick an end date.")
    st.stop()
start, end = picked

try:
    daily = data.load_daily(start, end, borough)
    zones = data.load_zones(start, end, borough, top_n)
except (data.QueryError, ValueError) as exc:
    st.error(str(exc), icon=":material/error:")
    st.stop()

if daily.empty:
    st.warning("No trips for this selection.")
    st.stop()

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

st.subheader("Trips by hour and weekday")
if st.toggle("Load heatmap (scans fct_trips)", value=True):
    try:
        heat = data.load_heatmap(start, end, borough)
    except (data.QueryError, ValueError) as exc:
        st.error(str(exc), icon=":material/error:")
        st.stop()
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
