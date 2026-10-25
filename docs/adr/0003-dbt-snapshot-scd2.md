# 0003. SCD2 vendor dimension with a dbt snapshot

Status: Accepted

## Context and problem

Vendor names can change. The fact table should join to the current vendor row, and the history should be kept.

## Options

- A plain table rebuilt from the seed (no history).
- A dbt snapshot with the `timestamp` strategy.
- A dbt snapshot with the `check` strategy.

## Decision

Snapshot `dim_vendor` from the `vendors` seed with `strategy='check'`, `check_cols=['vendor_name']` and `unique_key='vendor_id'`. The seed has no updated-at column, so `timestamp` is not possible.

The snapshot sets `table_type='iceberg'`. dbt-athena builds snapshots with MERGE, and Athena supports MERGE only on Iceberg tables.

`fct_trips` joins on `vendor_id` and `dbt_valid_to is null`, so it uses the current name only.

## Consequences

- History grows only when `dbt snapshot` runs. The DAG runs it before `dbt build` on every run that uploads a month. A run with nothing new skips it ([0009](0009-full-discovery.md)).
- The seed must be loaded (`dbt seed`) before the first snapshot. The DAG task `dbt_prepare` does this.
- Checked by hand once: renaming vendor 2 in the seed and rerunning `dbt snapshot` closed the old row and opened a new one. No automated test covers it.
- Facts do not carry the vendor name that was valid at trip time.
- Iceberg tables need Glue table-version reads and S3 writes under `warehouse/` (see [the runbook](../sop.md#troubleshooting)).
