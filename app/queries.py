"""SQL builders for the insight views. Pure functions, no I/O, no Streamlit.

Same safety rules as data.py: user values are validated, then bound through
Athena ExecutionParameters (`?`); SQL text only contains constants, validated
integers, and `?` placeholders. Every query has a LIMIT and a `pickup_month`
(partition column, YYYY-MM string) predicate where the table has one.

Day type uses `pickup_dow` as produced by `day_of_week()` in fct_trips.sql
(Trino/ISO numbering): 1 = Monday ... 7 = Sunday. Weekday = 1-5, Weekend = 6-7.
"""

import re
from dataclasses import dataclass
from datetime import date

from app.data import validate_borough, validate_limit, validate_range

MAX_CHOICES = 20
DAY_TYPES = ("All", "Weekday", "Weekend")
_DOW_SQL = {"Weekday": "pickup_dow between 1 and 5", "Weekend": "pickup_dow between 6 and 7"}
_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9 /.,()&_-]{0,39}")
AIRPORTS = ("JFK Airport", "LaGuardia Airport", "Newark Airport")

PAYMENTS_SQL = (
    "select distinct payment_type_name as value from mart_hourly_borough "
    "where payment_type_name is not null order by 1 limit 50"
)
VENDORS_SQL = (
    "select distinct vendor_name as value from mart_hourly_borough "
    "where vendor_name is not null order by 1 limit 50"
)


@dataclass(frozen=True)
class Filters:
    start: date
    end: date
    borough: str | None = None
    payments: tuple[str, ...] = ()
    vendors: tuple[str, ...] = ()
    hour_min: int = 0
    hour_max: int = 23
    day_type: str = "All"


def validate_names(values: tuple[str, ...]) -> tuple[str, ...]:
    if len(values) > MAX_CHOICES:
        raise ValueError(f"at most {MAX_CHOICES} choices")
    for v in values:
        if not isinstance(v, str) or not _NAME_RE.fullmatch(v):
            raise ValueError("invalid choice")
    return tuple(values)


def validate_hours(lo: int, hi: int) -> tuple[int, int]:
    ints = all(isinstance(h, int) and not isinstance(h, bool) for h in (lo, hi))
    if not ints or not 0 <= lo <= hi <= 23:
        raise ValueError("hour range must be 0 <= min <= max <= 23")
    return lo, hi


def _months(start: date, end: date) -> list[str]:
    return [f"'{start:%Y-%m}'", f"'{end:%Y-%m}'"]


def _in(column: str, values: tuple[str, ...]) -> tuple[str, list[str]]:
    return f" and {column} in ({', '.join('?' * len(values))})", [f"'{v}'" for v in values]


def hourly_where(f: Filters) -> tuple[str, list[str]]:
    start, end = validate_range(f.start, f.end)
    borough = validate_borough(f.borough)
    payments, vendors = validate_names(f.payments), validate_names(f.vendors)
    lo, hi = validate_hours(f.hour_min, f.hour_max)
    if f.day_type not in DAY_TYPES:
        raise ValueError("invalid day type")
    where = (
        "pickup_month between ? and ? "
        "and pickup_date between cast(? as date) and cast(? as date)"
        f" and pickup_hour between {lo} and {hi}"
    )
    params = [*_months(start, end), f"'{start.isoformat()}'", f"'{end.isoformat()}'"]
    if borough:
        where, params = where + " and pickup_borough = ?", [*params, f"'{borough}'"]
    for column, values in (("payment_type_name", payments), ("vendor_name", vendors)):
        if values:
            sql, vals = _in(column, values)
            where, params = where + sql, [*params, *vals]
    if f.day_type in _DOW_SQL:
        where += f" and {_DOW_SQL[f.day_type]}"
    return where, params


def _hourly(select: str, group: str, f: Filters, limit: int) -> tuple[str, list[str]]:
    where, params = hourly_where(f)
    sql = f"select {select} from mart_hourly_borough where {where} group by 1 order by 1 limit {limit}"
    return sql, params


_TIP = "sum(card_tip_revenue) as card_tip_revenue, sum(card_fare_revenue) as card_fare_revenue"
_EFF = (
    "sum(total_revenue) as total_revenue, sum(distance_miles_sum) as distance_miles_sum, "
    "sum(duration_min_sum) as duration_min_sum, sum(trips) as trips"
)


def build_tips_by_hour(f: Filters) -> tuple[str, list[str]]:
    return _hourly(f"pickup_hour, {_TIP}", "pickup_hour", f, 24)


def build_tips_by_borough(f: Filters) -> tuple[str, list[str]]:
    return _hourly(f"pickup_borough, {_TIP}", "pickup_borough", f, 50)


def build_efficiency_by_hour(f: Filters) -> tuple[str, list[str]]:
    return _hourly(f"pickup_hour, {_EFF}", "pickup_hour", f, 24)


def build_efficiency_by_borough(f: Filters) -> tuple[str, list[str]]:
    return _hourly(f"pickup_borough, {_EFF}", "pickup_borough", f, 50)


def build_airports_query(start: date, end: date) -> tuple[str, list[str]]:
    start, end = validate_range(start, end)
    zones = ", ".join(f"'{z}'" for z in AIRPORTS)
    sql = (
        f"select case when pickup_zone in ({zones}) then pickup_zone else 'Other' end as grp, "
        "sum(trips) as trips, sum(total_revenue) as revenue "
        "from mart_daily_zone_revenue "
        "where pickup_date between cast(? as date) and cast(? as date) group by 1 limit 10"
    )
    return sql, [f"'{start.isoformat()}'", f"'{end.isoformat()}'"]


def _od_where(start: date, end: date, borough: str | None) -> tuple[str, list[str]]:
    start, end = validate_range(start, end)
    borough = validate_borough(borough)
    where, params = "pickup_month between ? and ?", _months(start, end)
    if borough:
        where, params = where + " and pickup_borough = ?", [*params, f"'{borough}'"]
    return where, params


def build_top_routes_query(
    start: date, end: date, borough: str | None, top_n: int = 20
) -> tuple[str, list[str]]:
    where, params = _od_where(start, end, borough)
    n = validate_limit(top_n)
    sql = (
        "select pickup_zone, dropoff_zone, pickup_borough, dropoff_borough, "
        "sum(trips) as trips, sum(total_revenue) as total_revenue from mart_od_pairs "
        f"where {where} and pickup_zone is not null and dropoff_zone is not null "
        f"group by 1, 2, 3, 4 order by 5 desc limit {n}"
    )
    return sql, params


def build_borough_matrix_query(
    start: date, end: date, borough: str | None
) -> tuple[str, list[str]]:
    where, params = _od_where(start, end, borough)
    sql = (
        "select pickup_borough, dropoff_borough, sum(trips) as trips from mart_od_pairs "
        f"where {where} and pickup_borough is not null and dropoff_borough is not null "
        "group by 1, 2 order by 3 desc limit 100"
    )
    return sql, params


def build_quality_query(start: date, end: date) -> tuple[str, list[str]]:
    start, end = validate_range(start, end)
    sql = (
        "select month, raw_rows, staged_rows, dropped_rows, dropped_pct, "
        "implausible_speed_rows, built_at from mart_month_quality "
        "where month between ? and ? order by month limit 60"
    )
    return sql, _months(start, end)
