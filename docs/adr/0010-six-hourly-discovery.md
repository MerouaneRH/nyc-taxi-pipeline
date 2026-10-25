# 0010. Six-hourly discovery and batch ranges

Status: Accepted (replaces the `@monthly` schedule of [0009](0009-full-discovery.md); the rest of 0009 stands)

Notifications were part of the first version of this decision. The owner had them removed, and they are deferred until a channel is chosen.

## Context and problem

With `@monthly`, a month that TLC publishes late waits up to a month for the next run. A first load of a long history also runs as one unbounded task.

## Options

- Keep `@monthly`: cheap, but slow to pick up new months.
- Sensor on the TLC file: holds a worker slot and does not find old gaps (see [0009](0009-full-discovery.md)).
- Poll often with the existing discovery run: an empty run costs one S3 head and one HTTP HEAD per month, then skips dbt.

## Decision

- Schedule `0 */6 * * *` (every 6 hours). `start_date` 2024-01-01, `catchup=False`, `max_active_runs=1` and the retries stay.
- When nothing is new, `ingest_trips` exits 99 and is skipped, and so are `dbt_prepare`, `dbt_snapshot` and `dbt_build` ([0009](0009-full-discovery.md)). An empty run costs only the HEAD checks.
- New DAG param `ingest_until` (string `YYYY-MM`, default empty, meaning through the current month). When set, `ingest_trips` passes `--until <value>`. `ingestion.pipeline --until` needs `--since`, includes the named month, and is an error when it is before `--since` or badly formatted. It never reaches past the current UTC month. The DAG checks the format and `ingest_since <= ingest_until` when it renders the command (macro `until_flag`). An empty value adds no flag. To load a year in a batch, set `ingest_since` and `ingest_until`.

## Consequences

- New data is picked up within about 6 hours of TLC publishing it, instead of at the next month boundary.
- An idle run still makes two requests per month since `ingest_since` (about 34 months in October 2026), four times a day.
- The data interval of a 6-hourly run is 6 hours. The DAG uses `data_interval_start` for the run month and for `max_year` in both `dbt_snapshot` and `dbt_build`. The first run after a year boundary therefore still sees the old year as `max_year`. That matters only if a month of the new year is already published, which TLC's lag makes unlikely. In that case the `run_month` range check fails the build, and the next run succeeds.
- `ingest_trips` itself uses the current UTC date.
- Not verified: the live 6-hourly schedule and the `--until` batch trigger in a real Airflow run.
