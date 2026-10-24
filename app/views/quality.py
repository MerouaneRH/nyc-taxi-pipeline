import altair as alt
import pandas as pd
import streamlit as st

from app import dq_reasons, loaders
from app.queries import Filters
from app.views.common import fetch, scanned_caption
from app.views.quality_drill import render_drilldown

# (column in mart_month_quality, rule shown to the person)
RULES = (
    ("dropped_negative_total", "Negative total (refund or adjustment)"),
    ("dropped_dropoff_before_pickup", "Dropoff before pickup"),
    ("dropped_duration_ge_24h", "Trip lasted 24 hours or more"),
    ("dropped_pickup_outside_month", "Pickup outside the file's month"),
)


def headline_numbers(quality: pd.DataFrame, summary: pd.DataFrame) -> dict:
    raw, dropped = int(quality["raw_rows"].sum()), int(quality["dropped_rows"].sum())
    return {
        "raw": raw, "staged": int(quality["staged_rows"].sum()),
        "dropped_pct": round(100 * dropped / raw, 2) if raw else 0.0,
        "suspect": int(summary["trips"].sum()),
    }


def _headline(quality: pd.DataFrame, summary: pd.DataFrame) -> None:
    n = headline_numbers(quality, summary)
    cols = st.columns(4)
    cols[0].metric("Raw rows", f"{n['raw']:,}", border=True)
    cols[1].metric("Staged rows", f"{n['staged']:,}", border=True)
    cols[2].metric("Dropped", f"{n['dropped_pct']:.2f}%", border=True,
                   help="Share of raw rows removed before staging.")
    cols[3].metric("Suspect trips", f"{n['suspect']:,}", border=True,
                   help="Kept in the data, but flagged because a value looks broken.")


def _removed(quality: pd.DataFrame) -> None:
    st.subheader("What was removed")
    rules = pd.DataFrame({"Rule": [r for _, r in RULES],
                          "Rows": [int(quality[c].sum()) for c, _ in RULES]})
    st.dataframe(rules, hide_index=True, alt="Rows dropped by each rule",
                 column_config={"Rows": st.column_config.NumberColumn(format="localized")})
    neg = float(quality["negative_total_sum"].sum())
    st.markdown(
        f"Negative totals are **refunds or adjustments**, not bad records. They are "
        f"excluded from revenue; together they sum to **${neg:,.2f}**."
    )
    st.caption("One row can break several rules, so the counts can add up to more than the "
               f"dropped rows. Rows with a missing passenger count are kept "
               f"({int(quality['passenger_missing_rows'].sum()):,}).")


def _reason_table(summary: pd.DataFrame, staged: int) -> pd.DataFrame:
    df = summary.copy()
    df["pct_of_staged"] = 100 * df["trips"] / staged if staged else 0.0
    df["avg_total_amount"] = df["total_amount_sum"] / df["trips"].where(df["trips"] > 0)
    df["explanation"] = [dq_reasons.explain(r, f) for r, f in
                         zip(df["reason_code"], df["suspect_field"], strict=True)]
    return df


def _reason_chart(df: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(df).mark_bar().encode(
            y=alt.Y("reason_code:N", sort="-x", title=None),
            x=alt.X("trips:Q", title="Suspect trips"),
            tooltip=["reason_code", alt.Tooltip("trips:Q", format=","),
                     alt.Tooltip("total_amount_sum:Q", title="Money", format="$,.0f")],
        )
    )


def _by_reason(summary: pd.DataFrame, staged: int) -> None:
    st.subheader("Suspect trips by reason")
    st.caption("Suspect trips stay in revenue totals. Only speed and efficiency metrics "
               "should leave them out.")
    if summary.empty:
        st.info("No suspect trips for these months.")
        return
    df = _reason_table(summary, staged)
    st.dataframe(
        df, hide_index=True, alt="Suspect trips by reason with money on the flagged rows",
        column_order=["reason_code", "trips", "pct_of_staged", "total_amount_sum",
                      "avg_total_amount", "suspect_field", "explanation"],
        column_config={
            "reason_code": "Reason", "suspect_field": "Likely broken",
            "trips": st.column_config.NumberColumn("Trips", format="localized"),
            "pct_of_staged": st.column_config.NumberColumn("% of staged", format="%.3f%%"),
            "total_amount_sum": st.column_config.NumberColumn("Money on rows", format="dollar"),
            "avg_total_amount": st.column_config.NumberColumn("Avg total", format="dollar"),
            "explanation": st.column_config.TextColumn("What it means and what to do", width="large"),
        },
    )
    st.altair_chart(_reason_chart(df), alt="Bar chart of suspect trips by reason code")


def render(f: Filters) -> None:
    st.caption("Quality data is monthly: the date range is widened to whole months.")
    quality = fetch(loaders.load_quality, f.start, f.end)
    summary = fetch(loaders.load_dq_summary, f.start, f.end)
    if quality is None or summary is None:
        return
    if quality.empty:
        st.warning("No quality rows for these months.")
        return
    _headline(quality, summary)
    _removed(quality)
    _by_reason(summary, int(quality["staged_rows"].sum()))
    scanned_caption(quality, "Quality query")
    scanned_caption(summary, "Summary query")
    render_drilldown(f)
