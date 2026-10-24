-- Singular test: returns offending rows; passes when empty.
select trip_id, pickup_at, dropoff_at
from {{ ref('fct_trips') }}
where dropoff_at < pickup_at
