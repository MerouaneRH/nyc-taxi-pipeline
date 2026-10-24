"""SQL builders for the Data quality drill-down. Pure functions, no I/O, no Streamlit.

Same rules as queries.py: user values are validated (reason codes against a
whitelist, names against a regex) and bound through ExecutionParameters (`?`);
SQL text only holds constants. Every query has a LIMIT and a `pickup_month`
(partition column, YYYY-MM) predicate. Histogram buckets are computed in SQL.
"""

from datetime import date

from app.data import validate_range
from app.dq_reasons import REASON_CODES, validate_reason
from app.queries import _months, validate_names

EXAMPLE_LIMIT = 200
EXAMPLE_COLUMNS = (
    "trip_id, pickup_at, dropoff_at, trip_duration_min, trip_distance_miles, avg_speed_mph, "
    "passenger_count, fare_amount, tip_amount, total_amount, payment_type_name, vendor_name, "
    "pickup_zone, pickup_borough, dropoff_zone, dropoff_borough, reason_code, reason_codes, "
    "suspect_field"
)
# Most extreme rows first, per reason (constants, never user input).
_ORDER = {
    "SPEED_IMPLAUSIBLE": "avg_speed_mph desc",
    "DISTANCE_ABSURD": "trip_distance_miles desc",
    "ZERO_DISTANCE_PAID": "total_amount desc",
    "TIMESTAMP_SUSPECT": "trip_duration_min asc",
    "LONG_DURATION": "trip_duration_min desc",
    "ZERO_TOTAL": "trip_distance_miles desc",
}
# (bucket-start SQL expression, x-axis title) per reason; buckets are capped so
# the group-by stays small.
_DIST = {
    "SPEED_IMPLAUSIBLE": ("least(cast(floor(avg_speed_mph / 50) as integer), 20) * 50",
                          "Average speed, mph (bucket start, 1000+ pooled)"),
    "DISTANCE_ABSURD": ("cast(floor(log10(greatest(trip_distance_miles, 1))) as integer)",
                        "Distance, power of ten of miles (2 = 100 to 999 miles)"),
    "ZERO_DISTANCE_PAID": ("least(cast(floor(total_amount / 10) as integer), 20) * 10",
                           "Amount paid, USD (bucket start, 200+ pooled)"),
    "TIMESTAMP_SUSPECT": (
        "greatest(least(cast(floor(trip_duration_min / 10) as integer), 12), -12) * 10",
        "Duration, minutes (bucket start, negative = dropoff before pickup)"),
    "LONG_DURATION": ("least(cast(floor(trip_duration_min / 60) as integer), 72)",
                      "Duration, hours (bucket start, 72+ pooled)"),
    "ZERO_TOTAL": ("least(cast(floor(trip_distance_miles / 5) as integer), 12) * 5",
                   "Distance, miles (bucket start, 60+ pooled)"),
}
DIST_AXIS = {code: axis for code, (_, axis) in _DIST.items()}
assert set(_ORDER) == set(_DIST) == set(REASON_CODES)


def build_summary_query(start: date, end: date) -> tuple[str, list[str]]:
    start, end = validate_range(start, end)
    sql = (
        "select reason_code, suspect_field, sum(trips) as trips, "
        "sum(total_amount_sum) as total_amount_sum, min(example_trip_id) as example_trip_id "
        "from mart_dq_summary where pickup_month between ? and ? "
        "group by 1, 2 order by 3 desc limit 50"
    )
    return sql, _months(start, end)


def _name(value: str | None) -> str | None:
    return None if value is None else validate_names((value,))[0]


def build_examples_query(
    start: date, end: date, reason: str, vendor: str | None, payment: str | None
) -> tuple[str, list[str]]:
    start, end = validate_range(start, end)
    code = validate_reason(reason)
    where, params = "pickup_month between ? and ? and reason_code = ?", \
        [*_months(start, end), f"'{code}'"]
    for column, value in (("vendor_name", _name(vendor)), ("payment_type_name", _name(payment))):
        if value:
            where, params = f"{where} and {column} = ?", [*params, f"'{value}'"]
    sql = (f"select {EXAMPLE_COLUMNS} from dq_suspect_trips where {where} "
           f"order by {_ORDER[code]} limit {EXAMPLE_LIMIT}")
    return sql, params


def build_distribution_query(start: date, end: date, reason: str) -> tuple[str, list[str]]:
    start, end = validate_range(start, end)
    code = validate_reason(reason)
    sql = (
        f"select {_DIST[code][0]} as bucket, count(*) as trips from dq_suspect_trips "
        "where pickup_month between ? and ? and reason_code = ? group by 1 order by 1 limit 100"
    )
    return sql, [*_months(start, end), f"'{code}'"]
