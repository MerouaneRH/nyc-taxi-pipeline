import pytest

from app import dq_reasons

CODES = ["DISTANCE_ABSURD", "SPEED_IMPLAUSIBLE", "TIMESTAMP_SUSPECT",
         "ZERO_DISTANCE_PAID", "LONG_DURATION", "ZERO_TOTAL"]


def test_all_codes_explained():
    assert set(dq_reasons.REASONS) == set(CODES)
    assert tuple(dq_reasons.REASON_CODES) == tuple(dq_reasons.REASONS)


@pytest.mark.parametrize("code", CODES)
def test_explanation_has_meaning_field_and_handling(code):
    info = dq_reasons.REASONS[code]
    assert info.meaning and info.likely_field and info.handling
    assert "\n" not in info.meaning


def test_validate_reason_whitelist():
    assert dq_reasons.validate_reason("ZERO_TOTAL") == "ZERO_TOTAL"
    for bad in ("zero_total", "ZERO_TOTAL' or 1=1 --", "", None, 5):
        with pytest.raises(ValueError):
            dq_reasons.validate_reason(bad)


def test_one_line_summary_mentions_handling():
    line = dq_reasons.explain("SPEED_IMPLAUSIBLE", "timestamps")
    assert "timestamps" in line and "exclude" in line.lower()


def test_timestamp_suspect_text_matches_the_real_rule():
    # dbt: duration under dq_short_trip_seconds (60) and at least dq_short_trip_min_miles (1).
    # Staging already drops dropoff-before-pickup rows, so the text must not claim that.
    text = dq_reasons.REASONS["TIMESTAMP_SUSPECT"].meaning
    assert "60 seconds" in text and "1 mile" in text
    assert "at or before pickup" not in text


def test_handling_is_a_suggestion_not_a_dashboard_behaviour():
    from app.dq_reasons import KEEP_EXCLUDE

    assert "does not" in KEEP_EXCLUDE and "exclude" in KEEP_EXCLUDE.lower()
