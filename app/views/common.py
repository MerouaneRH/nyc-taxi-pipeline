"""Helpers shared by the views."""

from collections.abc import Callable

import pandas as pd
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError

from app import data


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
