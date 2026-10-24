import pandas as pd

from app import loaders
from app.views.quality import headline_numbers
from tests.app.test_views import LSTUBS, _attr, _run


def test_headline_numbers():
    q = pd.DataFrame({"raw_rows": [100, 300], "staged_rows": [98, 290], "dropped_rows": [2, 10]})
    s = pd.DataFrame({"trips": [5, 7]})
    assert headline_numbers(q, s) == {"raw": 400, "staged": 388, "dropped_pct": 3.0, "suspect": 12}


def test_headline_zero_raw_safe():
    q = pd.DataFrame({"raw_rows": [0], "staged_rows": [0], "dropped_rows": [0]})
    assert headline_numbers(q, pd.DataFrame({"trips": []}))["dropped_pct"] == 0.0


def test_section_order_and_content():
    at = _run("Data quality")
    assert not at.exception, at.exception
    heads = [h.value for h in at.subheader]
    assert heads == ["What was removed", "Suspect trips by reason", "Drill down", "Distribution"]
    assert [m.label for m in at.metric][:4] == ["Raw rows", "Staged rows", "Dropped", "Suspect trips"]
    text = " ".join(m.value for m in [*at.markdown, *at.caption])
    assert "refund" in text.lower() and "excluded from revenue" in text.lower()


def test_drilldown_selectbox_is_whitelist_and_default_reason():
    at = _run("Data quality")
    sb = next(s for s in at.selectbox if s.label == "Reason code")
    assert "SPEED_IMPLAUSIBLE" in sb.options and len(sb.options) == 6


def test_examples_loaded_with_selected_reason_and_filters():
    from contextlib import ExitStack
    from unittest.mock import patch

    from streamlit.testing.v1 import AppTest

    from app import data
    from tests.app.test_views import MAIN, STUBS

    with ExitStack() as stack:
        for n, v in STUBS.items():
            stack.enter_context(patch.object(data, n, return_value=v))
        for n, v in LSTUBS.items():
            stack.enter_context(patch.object(loaders, n, return_value=v))
        spy = stack.enter_context(patch.object(loaders, "load_dq_examples",
                                               return_value=LSTUBS["load_dq_examples"]))
        at = AppTest.from_file(str(MAIN), default_timeout=30).run()
        at.segmented_control[0].set_value("Data quality").run()
        assert not at.exception, at.exception
        assert spy.call_args.args[2:] == ("SPEED_IMPLAUSIBLE", None, None)
        next(s for s in at.selectbox if s.label == "Reason code").set_value("ZERO_TOTAL").run()
        assert spy.call_args.args[2] == "ZERO_TOTAL"


def test_download_button_and_dataframe_present():
    at = _run("Data quality")
    assert not at.exception
    assert at.dataframe
    assert at.get("download_button")


def test_empty_examples_shows_info_no_download():
    empty = _attr(LSTUBS["load_dq_examples"].iloc[0:0].copy())
    at = _run("Data quality", {"load_dq_examples": empty})
    assert not at.exception
    assert any("no suspect trips" in i.value.lower() for i in at.info)
    assert not at.get("download_button")


def test_scanned_mb_shown_for_each_query():
    at = _run("Data quality")
    scanned = [c.value for c in at.caption if "scanned" in c.value.lower()]
    assert len(scanned) >= 4 and all("MB" in c for c in scanned)


def test_missing_dbt_table_is_friendly():
    from app import data
    at = _run("Data quality", {"load_dq_summary": data.QueryError("TABLE_NOT_FOUND mart_dq_summary")})
    assert not at.exception
    assert len(at.error) == 1 and "TABLE_NOT_FOUND" not in at.error[0].value


def test_lazy_dq_loaders_not_called_outside_view():
    from unittest.mock import patch
    with patch.object(loaders, "load_dq_summary") as m:
        _run("Tips")
        assert not m.called
