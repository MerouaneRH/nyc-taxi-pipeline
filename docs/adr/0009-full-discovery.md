# 0009. Full discovery of TLC months

Status: Accepted (supersedes [0008](0008-catch-up-window-for-late-months.md))

The `@monthly` schedule mentioned here was replaced by [0010](0010-six-hourly-discovery.md). The rest of this decision stands.

## Context and problem

[0008](0008-catch-up-window-for-late-months.md) tried the last N months on each run. A month older than the window and missing from S3 was never loaded. For example, a bucket holding only 2024-01 to 2024-04 never received 2024-05 onward. The window also needed hand tuning. The pipeline should end up with every published month in S3 without a parameter that guesses how late TLC is.

## Options

- Fixed window ([0008](0008-catch-up-window-for-late-months.md)): few requests, but it misses anything older than N months and needs tuning.
- Full discovery: each run checks every month from a fixed start month through the current month and ingests whatever is published and missing (or different in size).
- Sensor: wait for a file to appear. It holds a worker slot, needs a timeout, blocks the next run under `max_active_runs=1`, and does not find old gaps.

## Decision

Full discovery.

- `ingestion.pipeline --since YYYY-MM` expands to every month from `since` through the current UTC month. It cannot be combined with `--months`. A `since` after the current month is a usage error.
- The newest month tried is the current month. A month that is not published returns 403 and is reported as unpublished. This is normal and does not stop the other months.
- `--dry-run` only runs the S3 head and HTTP HEAD checks. It prints `would-upload <month>`, `current <month>` or `unpublished <month>` per month, then the `NEW_MONTHS=` line, and exits with the code a real run would. It downloads and uploads nothing.
- The DAG param `ingest_window_months` becomes `ingest_since` (string `YYYY-MM`, default `2024-01`). It must match the format and must not be after the run month, or the task fails when the command renders.
- The `NEW_MONTHS` line, the exit 0 and 99 contract, `skip_on_exit_code=99`, the retries and the per-month skip rule ([0005](0005-idempotent-size-based-skip.md)) stay.
- `dbt_snapshot` and `dbt_build` both set `min_year` to the year of `ingest_since` and `max_year` to the year of the run. `dbt_build` still runs one build per new month, oldest first, with `run_month` set to the month. The wide year range lets `mart_month_quality` keep every month. The month filter on `run_month` still limits the incremental models to one partition.

## Consequences

- Requests per run grow with the months since the start month: one S3 head and one HTTP HEAD each (34 months in October 2026, counting from 2024-01). Months that are already current download nothing.
- A run that finds many new months runs one dbt build for each, about 2 minutes apiece, and downloads about 60 MB per month.
- The first run after this change backfills every missing month between `ingest_since` and the newest published month. Preview it with `--dry-run`.
- To start later and spend less, raise `ingest_since`. Months before it are never checked.
- The size-only skip limitation of [0005](0005-idempotent-size-based-skip.md) stays.
- A failed build in the loop stops it and fails the task. The retry repeats all months. The builds are safe to repeat, but a long backfill repeats work.
- Not verified: the XCom hand-off in a real Airflow run, and a real dbt backfill.
