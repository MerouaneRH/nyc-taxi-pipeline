import streamlit as st

from app import loaders
from app.queries import Filters
from app.views.common import fetch, scanned_caption


def render(f: Filters) -> None:
    df = fetch(loaders.load_quality, f.start, f.end)
    if df is None:
        return
    if df.empty:
        st.warning("No quality rows for these months.")
        return
    st.metric("Implausible-speed rows", f"{int(df['implausible_speed_rows'].sum()):,}",
              border=True)
    st.dataframe(df, hide_index=True, alt="Per-month row counts and drop rates")
    scanned_caption(df, "Query")
