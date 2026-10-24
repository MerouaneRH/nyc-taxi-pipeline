# 0003. SCD2 vendor dimension with a dbt snapshot

Status: Accepted

## Context and problem

Vendor names can change. The fact table should join to the current vendor row, and history should be kept.

## Options

- Plain table rebuilt from the seed (no history).
- dbt snapshot, `timestamp` strategy.
- dbt snapshot, `check` strategy.

## Decision

Snapshot `dim_vendor` from the `vendors` seed with `strategy='check'`, `check_cols=['vendor_name']`, `unique_key='vendor_id'`. The seed has no updated-at column, so `timestamp` is not possible.

The snapshot sets `table_type='iceberg'`. dbt-athena implements snapshots with MERGE, which Athena supports only on Iceberg tables.

`fct_trips` joins on `vendor_id` and `dbt_valid_to is null`, so it uses the current name only.

## Consequences

- History accumulates only when `dbt snapshot` runs. The DAG runs it each month before `dbt build`.
- The seed must be loaded (`dbt seed`) before the first snapshot. The DAG task `dbt_prepare` does this.
- Checked by hand once: renaming vendor 2 in the seed and re-running `dbt snapshot` closed the old row and opened a new one. There is no automated test for it.
- Facts do not carry the vendor name that was valid at trip time.
- Iceberg tables need Glue table-version reads and S3 writes under `warehouse/` (see `docs/sop.md`).
