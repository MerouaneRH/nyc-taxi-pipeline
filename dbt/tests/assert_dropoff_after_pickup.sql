-- Singular test: returns offending rows; passes when empty.
-- Staging already drops dropoff < pickup, so a raw-order check here could never fail.
-- Assert on the derived fct_trips metric instead: no negative duration.
-- (Implausible speeds are reported by assert_plausible_speed, severity warn.)
select trip_id, pickup_at, dropoff_at, trip_duration_min
from {{ ref('fct_trips') }}
where trip_duration_min < 0
