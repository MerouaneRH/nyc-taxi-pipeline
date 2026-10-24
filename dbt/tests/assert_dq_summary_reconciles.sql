-- Passes when empty: per month, distinct trips in dq_suspect_trips equal sum(trips) in mart_dq_summary.
with d as (
    select pickup_month, count(distinct trip_id) as n from {{ ref('dq_suspect_trips') }} group by 1
),
m as (
    select pickup_month, sum(trips) as n from {{ ref('mart_dq_summary') }} group by 1
)
select coalesce(d.pickup_month, m.pickup_month) as pickup_month, d.n as suspect_trips, m.n as summary_trips
from d full outer join m on d.pickup_month = m.pickup_month
where d.n is distinct from m.n
