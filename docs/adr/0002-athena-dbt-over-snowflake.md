# 0002. Athena with dbt-athena over Snowflake

Status: Accepted

## Context and problem

The dbt models need a SQL engine with a small, predictable cost and no extra account to set up.

## Options

- Snowflake (needs a new account).
- Athena with dbt-athena on the existing AWS account.

## Decision

Use Athena. It needs no extra account, costs nothing when idle, and bills by data scanned. Partition projection and the `min_year` and `max_year` variables in `dbt/dbt_project.yml` limit what a query scans.

## Consequences

- Models use Trino-style SQL (`date_diff`, `day_of_week`, `to_hex`, `to_utf8`).
- Marts are hive tables in parquet. The snapshot is an Iceberg table ([0003](0003-dbt-snapshot-scd2.md)).
- Athena reads parquet by column name. A month file with different column types can fail on read, so the year range is capped.
- Moving to Snowflake needs a new dbt adapter and profile, replacements for the Trino functions, and no Athena-specific `table_type` or `format` settings. The model layout, tests and DAG stay. This move was not tried.
- Each query costs scanned bytes plus S3 requests.
