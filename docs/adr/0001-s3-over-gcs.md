# 0001. S3 over GCS for the raw zone

Status: Accepted

## Context and problem

The raw TLC files need object storage. The project runs on an existing AWS account, and no GCP account is set up.

## Options

- S3 in `eu-west-3`.
- GCS.

## Decision

Use S3. The bucket holds `raw/yellow/year=YYYY/month=MM/`, `raw/zones/`, `athena-results/` and `warehouse/`.

## Consequences

- Athena, Glue and dbt-athena read the same bucket with no cross-cloud transfer.
- Credentials come from an AWS profile (`nyc-taxi`), not stored in the repo.
- The `year=/month=` layout is what the Glue partition projection template expects (`dbt/macros/create_raw_tables.sql`).
- Moving to GCS would change the ingestion client, the external table definitions and the query engine.
