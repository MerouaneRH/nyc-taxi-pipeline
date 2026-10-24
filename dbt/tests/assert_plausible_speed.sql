{{ config(severity='warn') }}
-- Warn-only: the TLC source has ~1.5k of 5.9M trips (1-second trips with distance)
-- above 200 mph. Reported, not blocking. avg_speed_mph is null for zero-length trips.
select trip_id, pickup_at, dropoff_at, trip_duration_min, avg_speed_mph
from {{ ref('fct_trips') }}
where avg_speed_mph >= 200
