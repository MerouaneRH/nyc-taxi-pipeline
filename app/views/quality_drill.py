"""Drill-down part of the Data quality view: example suspect trips and distribution."""

import altair as alt
import pandas as pd
import streamlit as st

from app import dq_reasons, loaders, queries_dq
from app.queries import Filters
from app.views.common import fetch, scanned_caption

DEFAULT_REASON = "SPEED_IMPLAUSIBLE"
SHOWN = ["pickup_at", "pickup_zone", "dropoff_zone", "trip_duration_min", "trip_distance_miles",
         "avg_speed_mph", "total_amount", "fare_amount", "tip_amount", "passenger_count",
         "payment_type_name", "vendor_name", "suspect_field", "reason_codes", "trip_id"]
HOW_TO_READ = (
    "How to read a row: compare duration, distance and speed. If one of them is "
    "impossible, that field is the likely broken one (see Suspect field). Money columns "
    "are as recorded and still count in revenue. Reason codes lists every rule the trip broke."
)


def _config() -> dict:
    nc = st.column_config.NumberColumn
    return {
        "pickup_at": st.column_config.DatetimeColumn("Pickup", format="YYYY-MM-DD HH:mm"),
        "pickup_zone": "Pickup zone", "dropoff_zone": "Dropoff zone",
        "trip_duration_min": nc("Duration (min)", format="%.1f"),
        "trip_distance_miles": nc("Distance (mi)", format="%.1f"),
        "avg_speed_mph": nc("Speed (mph)", format="%.0f"),
        "total_amount": nc("Total", format="dollar", width="medium"),
        "fare_amount": nc("Fare", format="dollar", width="medium"),
        "tip_amount": nc("Tip", format="dollar", width="medium"),
        "passenger_count": nc("Passengers", format="%d"),
        "payment_type_name": "Payment", "vendor_name": "Vendor",
        "suspect_field": "Suspect field", "reason_codes": "Reason codes", "trip_id": "Trip id",
    }


def _pick(label: str, options: list[str]) -> str | None:
    choice = st.selectbox(label, ["All", *options])
    return None if choice == "All" else choice


def _controls() -> tuple[str, str | None, str | None] | None:
    vendors, payments = fetch(loaders.load_vendors), fetch(loaders.load_payments)
    if vendors is None or payments is None:
        return None
    with st.container(horizontal=True):
        code = st.selectbox("Reason code", dq_reasons.REASON_CODES,
                            index=dq_reasons.REASON_CODES.index(DEFAULT_REASON))
        vendor, payment = _pick("Vendor", vendors), _pick("Payment type", payments)
    return code, vendor, payment


def _examples(f: Filters, code: str, vendor: str | None, payment: str | None) -> None:
    df = fetch(loaders.load_dq_examples, f.start, f.end, code, vendor, payment)
    if df is None:
        return
    if df.empty:
        st.info("No suspect trips for this reason and filter.")
    else:
        st.dataframe(df, hide_index=True, column_order=SHOWN, column_config=_config(),
                     alt=f"Example {code} trips, most extreme first")
        st.download_button("Download these rows (CSV)", df.to_csv(index=False),
                           file_name=f"suspect_{code.lower()}.csv", mime="text/csv",
                           icon=":material/download:")
        st.caption(f"Showing up to {queries_dq.EXAMPLE_LIMIT} trips, most extreme first. "
                   f"{HOW_TO_READ}")
    scanned_caption(df, "Examples query")


def distribution_chart(df: pd.DataFrame, code: str) -> alt.Chart:
    return (
        alt.Chart(df).mark_bar().encode(
            x=alt.X("bucket:O", title=queries_dq.DIST_AXIS[code], sort=None),
            y=alt.Y("trips:Q", title="Suspect trips", scale=alt.Scale(type="symlog")),
            tooltip=["bucket", alt.Tooltip("trips:Q", format=",")],
        )
    )


def _distribution(f: Filters, code: str) -> None:
    st.subheader("Distribution")
    df = fetch(loaders.load_dq_distribution, f.start, f.end, code)
    if df is None:
        return
    if df.empty:
        st.info("No suspect trips to chart for this reason.")
    else:
        st.caption("Counts use a log-like scale so rare extreme buckets stay visible. "
                   "Vendor and payment filters do not apply here.")
        st.altair_chart(distribution_chart(df, code),
                        alt=f"Histogram of {code} suspect trips")
    scanned_caption(df, "Distribution query")


def render_drilldown(f: Filters) -> None:
    st.subheader("Drill down")
    picked = _controls()
    if picked is None:
        return
    code, vendor, payment = picked
    st.caption(dq_reasons.explain(code))
    _examples(f, code, vendor, payment)
    _distribution(f, code)
