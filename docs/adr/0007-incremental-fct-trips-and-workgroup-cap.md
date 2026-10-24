# 0007. Incremental fct_trips by month and a capped Athena workgroup

Status: Accepted

## Context and problem

`fct_trips` was a full-year table. Each monthly DAG run reprocessed the whole `min_year..max_year` range, and the default `primary` workgroup has no scan limit, so a bad query or a widened year range could scan without bound.

## Options

- Keep the full rebuild.
- Hive table, `insert_overwrite` on a `pickup_month` partition.
- Iceberg table, `merge` on `trip_id`.

## Decision

- `fct_trips` is `incremental`, hive/parquet, `incremental_strategy=insert_overwrite`, `partitioned_by=['pickup_month']` (`YYYY-MM`, last column of the select). Insert-overwrite is idempotent per month and needs no unique-key lookup in the target.
- Var `run_month` (`YYYY-MM`) is validated in the model with a regex and must fall inside `min_year..max_year`; otherwise compilation fails. With `run_month` the model reads and rewrites one partition. Without it, an incremental run rewrites every partition in range. `--full-refresh` drops and rebuilds the table (fallback).
- `stg_trips` dedup window is now `partition by src_year, src_month, trip_id`. The result is the same (a trip_id embeds its pickup time, which must fall in the file month). The extra keys let the month filter push below the window to the raw partition scan.
- dbt uses Athena workgroup `nyc-taxi-dbt` (env `DBT_ATHENA_WORKGROUP`; the app reads `ATHENA_WORKGROUP`): engine v3, enforced configuration, results in `s3://<bucket>/athena-results/`, `BytesScannedCutoffPerQuery` = 5 GiB, CloudWatch metrics off. Created by `dbt/scripts/create_workgroup.py` (idempotent).
- Iceberg merge was not chosen: it needs a target scan to match `trip_id` and adds table maintenance. Not measured.

## Consequences

- Measured on 2024-01 and 2024-02 only (5,900,444 rows), workgroup `primary`: full refresh built `fct_trips` in 54.8 s and scanned 97 MB. Incremental `run_month=2024-02` took 55 to 61 s and scanned 49 MB (temp table) plus 145 MB (insert from the temp table). At two months of data the incremental run is not faster and scans more, because dbt-athena first writes `fct_trips__dbt_tmp` and then inserts from it. Cost per run is flat in the number of months, while a full rebuild grows with it. Break-even is expected at roughly three or four months of data; this was not measured.
- Partitioned hive output means many small files if months are tiny. Acceptable at this size.
- A scan cap fails a query that exceeds 5 GiB. Full rebuilds of many years may need a higher cap.
- Status of the workgroup: `athena:CreateWorkGroup` was denied for the `nyc-taxi` profile when this was written, so the workgroup did not exist. Until it is created (run the script with credentials that allow it), set `DBT_ATHENA_WORKGROUP=primary` and `ATHENA_WORKGROUP=primary`.
