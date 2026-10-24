from contextlib import ExitStack
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from botocore.exceptions import ClientError
from streamlit.testing.v1 import AppTest

from app import data

MAIN = Path(__file__).resolve().parents[2] / "app" / "main.py"
ARN ="arn:aws:iam::123456789012:role/secret"


def _boto():
    return ClientError({"Error": {"Code": "AccessDenied", "Message": ARN}}, "StartQueryExecution")


def _empty():
    return pd.DataFrame({"pickup_date": [], "trips": [], "revenue": [],
                         "fare_revenue": [], "tip_revenue": []})


def _run(**overrides):
    stubs = {"load_boroughs": ["Queens"], "load_bounds": (date(2024, 1, 1), date(2024, 2, 1)),
             "load_daily": _empty(), "load_zones": _empty()}
    stubs.update(overrides)
    with ExitStack() as stack:
        for name, val in stubs.items():
            kw = {"side_effect": val} if isinstance(val, Exception) else {"return_value": val}
            stack.enter_context(patch.object(data, name, **kw))
        return AppTest.from_file(str(MAIN), default_timeout=20).run()


def test_daily_boto_error_shows_generic_message():
    at = _run(load_daily=_boto())
    assert not at.exception
    assert len(at.error) == 1
    assert ARN not in at.error[0].value and "AccessDenied" not in at.error[0].value


def test_zones_boto_error_shows_generic_message():
    at = _run(load_zones=_boto())
    assert not at.exception
    assert len(at.error) == 1 and ARN not in at.error[0].value


def test_empty_mart_shows_info_not_crash():
    at = _run(load_bounds=None)
    assert not at.exception
    assert len(at.info) == 1 and not at.error
