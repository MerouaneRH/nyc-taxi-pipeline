-- Per-month data quality: raw vs staged rows, plus implausible speeds in fct_trips.
-- Partition predicates on year/month keep the raw scan to min_year..max_year.
-- Only months with raw data appear (a group-by over the raw partitions).
with raw_counts as (
    select year as src_year, month as src_month, count(*) as raw_rows
    from {{ source('raw', 'yellow_trips') }}
    where year between {{ var('min_year') | int }} and {{ var('max_year') | int }}
    group by 1, 2
),

staged as (
    select src_year, src_month, count(*) as staged_rows
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
    cast(current_timestamp as timestamp) as built_at
from raw_counts r
left join staged s on r.src_year = s.src_year and r.src_month = s.src_month
left join speed sp on format('%04d-%02d', r.src_year, r.src_month) = sp.pickup_month
