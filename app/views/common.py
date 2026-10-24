"""Helpers shared by the views."""

import logging
from collections.abc import Callable

import altair as alt
import pandas as pd
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError

from app import data

log = logging.getLogger(__name__)


def fetch(loader: Callable[..., pd.DataFrame], *args) -> pd.DataFrame | None:
    """Run a cached loader; show a UI-safe error and return None on failure."""
    try:
        return loader(*args)
    except (BotoCoreError, ClientError, data.QueryError, ValueError) as exc:
        st.error(data.friendly_error(exc), icon=":material/error:")
        return None


def scanned_caption(df: pd.DataFrame, label: str) -> None:
    st.caption(f"{label} scanned {df.attrs.get('scanned_bytes', 0) / 1e6:,.1f} MB.")


def scanned_total(*frames: pd.DataFrame) -> float:
    return sum(f.attrs.get("scanned_bytes", 0) for f in frames) / 1e6


def safe_render(render: Callable[..., None], *args) -> None:
    """Run a view; any unexpected error becomes a friendly message plus a server log entry."""
    try:
        render(*args)
    except Exception:
        log.exception("view %s failed", getattr(render, "__module__", "?"))
        st.error("This view failed unexpectedly. Details are in the server log.",
                 icon=":material/error:")


def hour_line_chart(df: pd.DataFrame, y: str, title: str) -> alt.Chart:
    """Line over hour of day 0-23 with a fixed x domain and a zero-based y axis."""
    return (
        alt.Chart(df, title=title)
        .mark_line(point=True)
        .encode(
            x=alt.X("pickup_hour:Q", title="Hour of day",
                    scale=alt.Scale(domain=[0, 23], nice=False),
                    axis=alt.Axis(tickMinStep=1, format="d")),
            y=alt.Y(f"{y}:Q", title=y.replace("_", " "), scale=alt.Scale(zero=True, nice=True)),
            tooltip=["pickup_hour", alt.Tooltip(f"{y}:Q", format=",.2f")],
        )
    )
