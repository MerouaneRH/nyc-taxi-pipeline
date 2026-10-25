# 0008. Catch-up window for late-published months

Status: Superseded by [0009](0009-full-discovery.md)

Superseded by 0009: the fixed window is replaced by full discovery from a start month.

## Context and problem

TLC publishes a month about two months late. The DAG run for data month M fires at the start of M+1. `ingest_trips` finds M unpublished (CloudFront returns 403) and the task is skipped through exit code 99 and `skip_on_exit_code=99`. Nothing retried M later, so M stayed missing.

## Options

- Lag by two months: the run processes the month two months back. Simple, but the lag is a guess; if TLC is later, the month is still missed, and if earlier, data waits for no reason.
- Catch-up window: each run tries the last N months. Ingestion already skips months whose S3 object has the same size as the source, so months already loaded cost one S3 head and one HTTP HEAD each. A late month is picked up by a later run.
- Sensor: wait for the file to appear. Holds a worker slot (or needs rescheduling mode), needs a timeout, and with `max_active_runs=1` a waiting run blocks the next one.

## Decision

Catch-up window.

- DAG param `ingest_window_months` (integer, default 4, minimum 1, maximum 12). `ingest_trips` passes the window months, newest first, computed in Jinja from `data_interval_start`.
- `ingestion.pipeline --skip-unpublished` processes every month and prints `NEW_MONTHS=<uploaded months, oldest first>` as the last stdout line. Exit 0 if at least one month was uploaded; exit 99 if none was (all current, or unpublished). Unpublished months are logged on stderr as before.
- `dbt_build` reads `NEW_MONTHS` from the XCom of `ingest_trips` and runs one incremental build per new month, oldest first, with that month's year for `min_year`/`max_year` and `run_month` set to the month.

## Consequences

- A late month is loaded and built by the first run after TLC publishes it, as long as it is still inside the window.
- A run with nothing new exits 99, so `dbt_prepare`, `dbt_snapshot` and `dbt_build` are skipped. The snapshot (SCD2 on vendors) therefore only advances on runs that upload something. To rebuild a month without new data, run dbt by hand (docs/sop.md).
- `dbt_snapshot` still uses the run's year for `min_year`/`max_year`, not the years of the new months.
- The size-only skip limitation of ADR 0005 remains: a source file replaced by TLC with the same size is not re-ingested.
- The window is a number to tune. Each extra month adds two cheap requests per run. If TLC lag exceeds N-1 months the month is missed again; raise the window.
- If one build in the `dbt_build` loop fails, the loop stops with a non-zero exit and the task retries all months; the builds are idempotent per month.
- Not verified here: the XCom hand-off inside a real Airflow run (tests render the commands and run the `dbt_build` loop against a stub `dbt`).
