# 0008. Catch-up window for late-published months

Status: Superseded by [0009](0009-full-discovery.md)

The fixed window described here was replaced by full discovery from a start month.

## Context and problem

TLC publishes a month about two months late. The monthly run for month M started at the beginning of M+1, found M unpublished (CloudFront answers 403), and skipped the task through exit code 99. Nothing tried M again, so M stayed missing.

## Options

- Lag by two months: each run processes the month two months back. This is simple, but the lag is a guess. If TLC is later, the month is still missed. If TLC is earlier, data waits for no reason.
- Catch-up window: each run tries the last N months. Ingestion already skips months whose S3 size matches the source, so a loaded month costs one S3 head and one HTTP HEAD. A late month is picked up by a later run.
- Sensor: wait for the file to appear. A sensor holds a worker slot (or needs reschedule mode) and needs a timeout. With `max_active_runs=1`, a waiting run blocks the next one.

## Decision

Catch-up window.

- DAG param `ingest_window_months` (integer, default 4, from 1 to 12). `ingest_trips` passes the window's months, newest first, computed in Jinja from `data_interval_start`.
- `ingestion.pipeline --skip-unpublished` processes every month and prints `NEW_MONTHS=<uploaded months, oldest first>` as its last stdout line. It exits 0 if at least one month was uploaded and 99 if none was.
- `dbt_build` reads `NEW_MONTHS` from the XCom of `ingest_trips` and runs one incremental build per new month, oldest first, with `run_month` set to the month.

## Consequences

- A late month is loaded and built by the first run after TLC publishes it, as long as it is still inside the window.
- A run with nothing new exits 99, so `dbt_prepare`, `dbt_snapshot` and `dbt_build` are skipped.
- A month older than the window and missing from S3 is never loaded. This is why [0009](0009-full-discovery.md) replaced the window.
- The size-only skip limitation of [0005](0005-idempotent-size-based-skip.md) stays.
- If one build in the `dbt_build` loop fails, the loop stops, the task fails, and the retry repeats all months. The builds are safe to repeat.
