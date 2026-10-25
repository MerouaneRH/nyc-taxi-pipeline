# 0010. Six-hourly discovery and batch ranges

Status: Accepted (supersedes the `@monthly` schedule of [0009](0009-full-discovery.md); the rest of 0009 stands). The notification part was removed at the owner's request; it is deferred until a channel is chosen.

## Context and problem

With `@monthly` a month that TLC publishes late waits up to a month for the next run, and nobody learns that data arrived or that a task failed unless they open Airflow. A first load of a long history also runs as one unbounded task.

## Options

- Keep `@monthly`: cheap, but slow to pick up new months and silent.
- Sensor on the TLC file: holds a worker slot and does not find old gaps (see 0009).
- Poll often with the existing discovery run: an empty run costs one S3 head and one HTTP HEAD per month and then skips dbt.

## Decision

- Schedule `0 */6 * * *` (every 6 hours). `start_date` 2024-01-01, `catchup=False`, `max_active_runs=1` and retries are unchanged.
- When nothing is new, `ingest_trips` exits 99 and is skipped, and `dbt_prepare`, `dbt_snapshot` and `dbt_build` are skipped (ADR 0009). An empty run costs only the HEAD checks.
- New DAG param `ingest_until` (string `YYYY-MM`, default empty = through the current month). When set, `ingest_trips` passes `--until <value>`. `ingestion.pipeline --until` needs `--since`, is inclusive, and is an error if before `--since` or badly formatted. It never reaches past the current UTC month. The DAG validates format and `ingest_since <= ingest_until` at render time (macro `until_flag`); an empty value renders no flag. A year can be loaded in a batch with `ingest_since` and `ingest_until`.

## Consequences

- New data is picked up within about 6 hours of TLC publishing it, instead of at the next month boundary.
- Each run makes two HTTP-level checks per month since `ingest_since` (about 34 months in October 2026) even when idle, four times a day.
- The data interval of a 6-hourly run is 6 hours. `data_interval_start` is used for the run month and snapshot `max_year`; in the first 6 hours after a month or year boundary the run month is still the previous one. `ingest_trips` itself uses the current UTC date, and `dbt_build` uses each month's own year, so new months are still loaded and built; only `dbt_snapshot` `max_year` can lag until the next run.
- Not verified: the live 6-hourly schedule and the `--until` batch trigger in a real Airflow run.
