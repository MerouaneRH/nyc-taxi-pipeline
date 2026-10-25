# 0009. Full discovery of TLC months

Status: Accepted (supersedes [0008](0008-catch-up-window-for-late-months.md))

Note: the `@monthly` schedule described here is superseded by [0010](0010-six-hourly-discovery-and-notifications.md) for scheduling only.

## Context and problem

ADR 0008 tried the last N months on each run. A month older than the window that was missing in S3 (for example 2024-05 to 2026-08 when the bucket only held 2024-01 to 2024-04) was never loaded, and the window had to be tuned by hand. The pipeline should converge on "every month TLC has published is in S3" without a parameter that guesses how late TLC is.

## Options

- Fixed window (0008): few requests, but misses anything older than N months and needs tuning.
- Full discovery: each run checks every month from a fixed start month through the current month and ingests whatever is published and missing (or different in size). No guess about TLC lag.
- Sensor: wait for a file to appear. Holds a worker slot, needs a timeout, and with `max_active_runs=1` a waiting run blocks the next one. It also does not find old gaps.

## Decision

Full discovery.

- `ingestion.pipeline --since YYYY-MM` expands to every month from `since` (inclusive) through the current UTC month. It is mutually exclusive with `--months`, which works as before. A `since` after the current month is a usage error.
- The newest month tried is the current month. A month not yet published returns 403 and is reported as unpublished, which is normal and does not stop the rest.
- `--dry-run` does only the S3 head and HTTP HEAD checks and prints `would-upload <month>`, `current <month>` or `unpublished <month>` per month, then the `NEW_MONTHS=` line and exit code a real run would give. It downloads and uploads nothing.
- The DAG param `ingest_window_months` is replaced by `ingest_since` (string `YYYY-MM`, default `2024-01`). It is validated against the format and must not be after the run month; otherwise the task fails at render time.
- The NEW_MONTHS line, exit 0 / 99 contract, `skip_on_exit_code=99`, retries and the per-month skip rule (ADR 0005) are unchanged.
- `dbt_snapshot` uses `min_year` = year of `ingest_since` and `max_year` = year of the run, so a backfill is covered. `dbt_build` still runs one build per new month, oldest first, each with its own year as `min_year`/`max_year` and `run_month` set to the month: a single month only needs its own partitions.

## Consequences

- The number of requests per run grows with the months since the start month: one S3 head plus one HTTP HEAD per month (34 months in October 2026 from 2024-01). No download happens for months already current.
- A run that finds many new months runs many dbt builds, one per month, about 2 minutes each, and downloads about 60 MB per month.
- The first run after this change deploys backfills every missing month between `ingest_since` and the newest published month. Preview with `--dry-run` first.
- To start later (cheaper), raise `ingest_since`. Months before it are never checked.
- The size-only skip limitation of ADR 0005 remains: a source file replaced by TLC with the same size is not re-ingested.
- A failed build in the loop stops it with a non-zero exit and the task retries all months; builds are idempotent per month, but a long backfill repeats work on retry.
- Not verified: the XCom hand-off in a real Airflow run and a real dbt backfill.
