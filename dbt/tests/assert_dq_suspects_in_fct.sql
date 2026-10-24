-- Passes when empty: every dq_suspect_trips trip_id exists in fct_trips (same month).
select s.trip_id, s.pickup_month
from {{ ref('dq_suspect_trips') }} s
left join {{ ref('fct_trips') }} f on s.trip_id = f.trip_id and s.pickup_month = f.pickup_month
where f.trip_id is null
