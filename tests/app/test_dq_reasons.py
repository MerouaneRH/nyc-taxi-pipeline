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
