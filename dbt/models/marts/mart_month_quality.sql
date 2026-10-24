-- Per-month data quality: raw vs staged rows, plus implausible speeds in fct_trips.
-- Partition predicates on year/month keep the raw scan to min_year..max_year.
-- Only months with raw data appear (a group-by over the raw partitions).
with raw_counts as (
    select year as src_year, month as src_month, count(*) as raw_rows,
        -- non-exclusive: a row can match several drop rules (see stg_trips `valid`)
        count_if(total_amount < 0) as dropped_negative_total,
        coalesce(sum(case when total_amount < 0 then total_amount end), 0) as negative_total_sum,
        count_if(tpep_dropoff_datetime < tpep_pickup_datetime) as dropped_dropoff_before_pickup,
        count_if(tpep_dropoff_datetime >= tpep_pickup_datetime
                 and date_diff('hour', tpep_pickup_datetime, tpep_dropoff_datetime) >= 24) as dropped_duration_ge_24h,
        count_if(tpep_pickup_datetime is not null
                 and (year(tpep_pickup_datetime) <> year or month(tpep_pickup_datetime) <> month)) as dropped_pickup_outside_month
    from {{ source('raw', 'yellow_trips') }}
    where year between {{ var('min_year') | int }} and {{ var('max_year') | int }}
    group by 1, 2
),

staged as (
    select src_year, src_month, count(*) as staged_rows,
        count_if(passenger_count is null or passenger_count = 0) as passenger_missing_rows
    from {{ ref('stg_trips') }}
    group by 1, 2
),

speed as (
    select pickup_month, count(*) as implausible_speed_rows
    from {{ ref('fct_trips') }}
    where avg_speed_mph >= 200
    group by 1
)

select
    format('%04d-%02d', r.src_year, r.src_month) as month,
    r.raw_rows,
    coalesce(s.staged_rows, 0) as staged_rows,
    r.raw_rows - coalesce(s.staged_rows, 0) as dropped_rows,
    100.0 * (r.raw_rows - coalesce(s.staged_rows, 0)) / r.raw_rows as dropped_pct,
    coalesce(sp.implausible_speed_rows, 0) as implausible_speed_rows,
    cast(current_timestamp as timestamp) as built_at,
    r.dropped_negative_total,
    r.negative_total_sum,
    r.dropped_dropoff_before_pickup,
    r.dropped_duration_ge_24h,
    r.dropped_pickup_outside_month,
    coalesce(s.passenger_missing_rows, 0) as passenger_missing_rows
from raw_counts r
left join staged s on r.src_year = s.src_year and r.src_month = s.src_month
left join speed sp on format('%04d-%02d', r.src_year, r.src_month) = sp.pickup_month
