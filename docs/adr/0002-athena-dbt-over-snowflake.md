# 0002. Athena with dbt-athena over Snowflake

Status: Accepted

## Context and problem

The project needs a SQL engine for dbt models, with a small, predictable cost and no extra account to provision.

## Options

- Snowflake (new account needed).
- Athena with dbt-athena on the existing AWS account.

## Decision

Use Athena. It needs no extra account, has no idle cost, and bills per data scanned. Scan cost is limited by partition projection and the `min_year`/`max_year` vars in `dbt/dbt_project.yml`.

## Consequences

- Models use Trino-style SQL (`date_diff`, `day_of_week`, `to_hex`, `to_utf8`).
- Marts are hive/parquet tables. The snapshot is Iceberg (ADR 0003).
- Athena reads parquet by column name. Month files with different physical types can fail on read, so the year range is capped.
- Moving to Snowflake needs a new dbt adapter and profile, function replacements, and removal of the Athena-specific `table_type`/`format` configs. The model layout, tests and DAG stay. Not tested.
- Each query costs scanned bytes plus S3 requests.
