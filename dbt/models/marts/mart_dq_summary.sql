-- One row per month and primary reason code. `trips` counts each suspect trip once (under its primary
-- reason_code), so sum(trips) per month = distinct trips in dq_suspect_trips. suspect_field here is the
-- code's own field; individual trips in dq_suspect_trips may be 'both'.
with month_trips as (
    select pickup_month, count(*) as month_trips
    from {{ ref('fct_trips') }}
    group by 1
)

select
    s.pickup_month,
    s.reason_code,
    case s.reason_code
        when 'DISTANCE_ABSURD' then 'distance'
        when 'SPEED_IMPLAUSIBLE' then 'distance or timestamps'
        when 'TIMESTAMP_SUSPECT' then 'timestamps'
        when 'ZERO_DISTANCE_PAID' then 'distance'
        when 'LONG_DURATION' then 'timestamps'
        when 'ZERO_TOTAL' then 'fare'
    end as suspect_field,
    count(*) as trips,
    100.0 * count(*) / max(m.month_trips) as pct_of_month_trips,
    sum(s.total_amount) as total_amount_sum,
    avg(s.total_amount) as avg_total_amount,
    min(s.trip_id) as example_trip_id
from {{ ref('dq_suspect_trips') }} s
join month_trips m on s.pickup_month = m.pickup_month
group by s.pickup_month, s.reason_code
