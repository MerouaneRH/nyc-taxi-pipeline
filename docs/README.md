# Documentation

Read in this order.

1. [Architecture](architecture.md): how data moves from TLC to the dashboard, with diagrams of the system, the dbt lineage and the Airflow DAG.
2. [Data model](data-model.md): every dbt model, the tests, and the rules that flag suspect trips.
3. [Runbook](sop.md): commands for loading months, rebuilding, checking failed runs, rotating credentials and reading the data-quality page.
4. Decision records, one file per decision:
   - [0001 S3 over GCS](adr/0001-s3-over-gcs.md)
   - [0002 Athena and dbt-athena over Snowflake](adr/0002-athena-dbt-over-snowflake.md)
   - [0003 dbt snapshot for SCD2](adr/0003-dbt-snapshot-scd2.md)
   - [0004 Streamlit over Tableau](adr/0004-streamlit-over-tableau.md)
   - [0005 Idempotent ingestion with a size-based skip](adr/0005-idempotent-size-based-skip.md)
   - [0006 CI without AWS secrets](adr/0006-ci-without-aws-secrets.md)
   - [0007 Incremental fct_trips and a capped workgroup](adr/0007-incremental-fct-trips-and-workgroup-cap.md)
   - [0008 Catch-up window for late months](adr/0008-catch-up-window-for-late-months.md) (superseded by 0009)
   - [0009 Full discovery of TLC months](adr/0009-full-discovery.md) (schedule replaced by 0010)
   - [0010 Six-hourly discovery and batch ranges](adr/0010-six-hourly-discovery.md)

The project overview and quick start are in the [top-level README](../README.md).
