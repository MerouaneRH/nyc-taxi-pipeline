"""Plain-language explanation of each suspect-trip reason code. Pure data, no I/O.

`REASON_CODES` is the whitelist for every reason code that reaches SQL.
"""

from dataclasses import dataclass

KEEP_EXCLUDE = (
    "Suggested: keep in revenue and exclude from speed and efficiency analysis. "
    "The dashboard does not exclude these trips for you."
)


@dataclass(frozen=True)
class ReasonInfo:
    meaning: str
    likely_field: str
    handling: str


REASONS: dict[str, ReasonInfo] = {
    "DISTANCE_ABSURD": ReasonInfo(
        "Trip distance is impossibly large for New York.", "distance", KEEP_EXCLUDE),
    "SPEED_IMPLAUSIBLE": ReasonInfo(
        "Distance divided by duration gives a speed no taxi can reach.",
        "distance or timestamps", KEEP_EXCLUDE),
    "TIMESTAMP_SUSPECT": ReasonInfo(
        "The trip lasted under 60 seconds but covers at least 1 mile.",
        "timestamps", KEEP_EXCLUDE),
    "ZERO_DISTANCE_PAID": ReasonInfo(
        "The meter recorded 0 miles but the passenger paid a fare.", "distance", KEEP_EXCLUDE),
    "LONG_DURATION": ReasonInfo(
        "Trip lasted many hours, likely a meter left running.", "timestamps", KEEP_EXCLUDE),
    "ZERO_TOTAL": ReasonInfo(
        "Total charged is exactly 0, so there is no revenue to count.", "fare",
        "Nothing to add to revenue; exclude from per-mile and per-minute metrics."),
}
REASON_CODES: tuple[str, ...] = tuple(REASONS)


def validate_reason(code: object) -> str:
    if not isinstance(code, str) or code not in REASONS:
        raise ValueError("invalid reason code")
    return code


def explain(code: str, suspect_field: str | None = None) -> str:
    """One-line explanation: meaning, likely broken field, suggested handling."""
    info = REASONS[validate_reason(code)]
    field = suspect_field or info.likely_field
    return f"{info.meaning} Likely broken field: {field}. {info.handling}"
