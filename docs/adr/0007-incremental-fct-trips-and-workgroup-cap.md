# 0007. Incremental fct_trips by month and a capped Athena workgroup

Status: Accepted

## Context and problem

`fct_trips` was a full-year table, so each DAG run reprocessed the whole `min_year` to `max_year` range. The default `primary` workgroup has no scan limit, so a bad query or a wider year range could scan without bound.

## Options

- Keep the full rebuild.
- Hive table with `insert_overwrite` on a `pickup_month` partition.
- Iceberg table with `merge` on `trip_id`.

## Decision

- `fct_trips` is `incremental`, hive and parquet, with `incremental_strategy=insert_overwrite` and `partitioned_by=['pickup_month']` (`YYYY-MM`, the last column of the select). Insert-overwrite gives the same result on a rerun and needs no key lookup in the target table.
- The variable `run_month` (`YYYY-MM`) is checked in the model with a regex and must fall inside `min_year` to `max_year`, or compilation fails. With `run_month`, the model reads and rewrites one partition. Without it, an incremental run rewrites every partition in range. `--full-refresh` drops and rebuilds the table.
- The `stg_trips` dedup window is `partition by src_year, src_month, trip_id`. The result is the same, because a `trip_id` contains its pickup time, which must fall in the file's month. The extra keys let the month filter reach the raw partition scan.
- dbt uses the Athena workgroup `nyc-taxi-dbt` (env `DBT_ATHENA_WORKGROUP`; the app reads `ATHENA_WORKGROUP`). It runs engine version 3, enforces its configuration, writes results to `s3://<bucket>/athena-results/`, caps each query at 5 GiB scanned (`BytesScannedCutoffPerQuery`) and has CloudWatch metrics off. `dbt/scripts/create_workgroup.py` creates it and is safe to rerun.
- Iceberg `merge` was not chosen. It needs a scan of the target to match `trip_id` and adds table maintenance. This was not measured.

Later, `mart_hourly_borough`, `mart_od_pairs` and `dq_suspect_trips` adopted the same pattern.

## Consequences

- Measured on 2024-01 and 2024-02 only (5,900,444 rows), in the `primary` workgroup: a full refresh built `fct_trips` in 54.8 s and scanned 97 MB. An incremental run with `run_month=2024-02` took 55 to 61 s and scanned 49 MB (temporary table) plus 145 MB (insert from the temporary table). At two months of data the incremental run is not faster and scans more, because dbt-athena first writes `fct_trips__dbt_tmp` and then inserts from it. The cost of an incremental run stays flat as months pile up, while a full rebuild grows. The break-even is expected at three or four months. This was not measured.
- Partitioned hive output makes many small files when months are small. That is acceptable at this size.
- A query above 5 GiB fails. A full rebuild of many years may need a higher cap.
- The workgroup `nyc-taxi-dbt` exists with the cap enforced. The measurements above predate it.
