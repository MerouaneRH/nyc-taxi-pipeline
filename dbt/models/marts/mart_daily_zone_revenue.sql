-- Dashboard mart: revenue per pickup zone per day.
select
    pickup_date,
    pickup_zone_id,
    pickup_borough,
    pickup_zone,
    count(*) as trips,
    sum(total_amount) as total_revenue,
    sum(fare_amount) as fare_revenue,
    sum(tip_amount) as tip_revenue,
    avg(trip_distance_miles) as avg_distance_miles,
    avg(trip_duration_min) as avg_duration_min
from {{ ref('fct_trips') }}
group by 1, 2, 3, 4
