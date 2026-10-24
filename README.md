# NYC Taxi ELT

Batch ELT pipeline for NYC TLC yellow taxi trip data. Python loads monthly
parquet files into S3, Athena reads them in place through Glue external tables,
dbt-athena builds the models, Airflow runs the monthly sequence, and a
Streamlit app is the planned consumer of the mart.

## Architecture

```mermaid
flowchart LR
    TLC[TLC CloudFront<br/>parquet + zone CSV] --> ING[Python ingestion<br/>ingestion/pipeline.py]
    ING --> S3[(S3 raw<br/>raw/yellow/year=YYYY/month=MM)]
    S3 --> GLUE[Glue catalog + Athena<br/>external tables, partition projection]
    GLUE --> STG[dbt staging<br/>stg_trips, stg_zones]
    SEED[seed vendors.csv] --> SNAP[dim_vendor<br/>SCD2 snapshot, Iceberg]
    STG --> DZ[dim_zone]
    STG --> FCT[fct_trips]
    DZ --> FCT
    SNAP --> FCT
    FCT --> MART[mart_daily_zone_revenue]
    MART --> ST[Streamlit app<br/>not in repo yet]
    AF{{Airflow<br/>@monthly DAG}} -.orchestrates.-> ING
    AF -.-> STG
    DC[Docker Compose<br/>local Airflow + Postgres] -.runs.-> AF
    GH[GitHub Actions CI<br/>no AWS secrets] -.checks.-> ING
```

Airflow task order (`dags/nyc_taxi_elt.py`):
`ingest_zones >> ingest_trips >> dbt_snapshot >> dbt_build`.

## Stack

| Layer | Tool |
|---|---|
| Source | TLC public files on CloudFront (`d37ci6vzurychx.cloudfront.net`) |
| Ingestion | Python 3.12, requests, boto3, pyarrow, tenacity |
| Storage | S3, region `eu-west-3` |
| Query | Athena over Glue external tables (partition projection on year, month) |
| Transform | dbt-core 1.12.6, dbt-athena 1.11.1 |
| Orchestration | Airflow 2.10.5, LocalExecutor, Postgres 16.4 |
| CI | GitHub Actions (ruff, pytest, DAG import tests, dbt parse) |
| BI | Streamlit (stub Dockerfile only) |

## Quickstart

Prerequisites: Python 3.12, `uv`, Docker, an AWS profile named `nyc-taxi`
with access to the bucket, Glue and Athena. Required IAM actions are listed in
`docs/sop.md`.

```bash
# 1. Python deps and tests (no AWS needed)
uv sync
uv run pytest tests --ignore=tests/dags
uv run ruff check .

# 2. Load one month into S3
export AWS_PROFILE=nyc-taxi
uv run python -m ingestion.pipeline --zones
uv run python -m ingestion.pipeline --service yellow --months 2024-01

# 3. Create Glue tables, then build models (from dbt/)
cd dbt
dbt deps --profiles-dir . --target athena
dbt run-operation create_raw_tables --profiles-dir . --target athena
dbt seed     --profiles-dir . --target athena
dbt snapshot --profiles-dir . --target athena
dbt build    --profiles-dir . --target athena
```

dbt needs `dbt-core` and `dbt-athena` installed. The `dbt` dependency group in
`pyproject.toml` lists them, so use `uv sync --group dbt` and prefix commands
with `uv run`.

Local Airflow:

```bash
cp .env.example .env        # set POSTGRES_PASSWORD and AIRFLOW_ADMIN_PASSWORD
docker compose up --build   # UI at http://localhost:8080
```

The DAG `nyc_taxi_elt` is created paused. Compose mounts `~/.aws` read-only
into the containers. The compose file requires `POSTGRES_PASSWORD` and
`AIRFLOW_ADMIN_PASSWORD`, and fails at startup if they are unset.

## Repo layout

```
ingestion/
  pipeline.py   TLC -> S3 loader used by the DAG (idempotent, retries, schema check)
  download.py   local download of TLC files to data/raw (also provides trip_url)
  soda.py       local pull from the NYC Open Data SODA API to data/raw/soda
dbt/
  models/staging/   stg_trips, stg_zones
  models/marts/     dim_zone, fct_trips, mart_daily_zone_revenue
  snapshots/        dim_vendor (SCD2, Iceberg)
  seeds/            vendors.csv
  macros/           create_raw_tables (Glue database + external tables)
  tests/            assert_dropoff_after_pickup.sql
  profiles.yml      Athena target, no secrets
dags/nyc_taxi_elt.py
docker/             airflow.Dockerfile, streamlit.Dockerfile (stub)
docker-compose.yml  postgres, airflow-init, webserver, scheduler
tests/              ingestion unit tests, DAG import tests
notebooks/          exploration notebook
.github/workflows/ci.yml
docs/               architecture.md, sop.md, adr/
```

## Data model

| Model | Type | Notes |
|---|---|---|
| `stg_trips` | view | Casts and renames; drops invalid rows; `trip_id` is an md5 of vendor, pickup, dropoff, zones, total; dedups on `trip_id` |
| `stg_zones` | view | Zone lookup CSV with typed `zone_id` |
| `dim_zone` | table (hive, parquet) | Zones from `stg_zones` |
| `dim_vendor` | snapshot (Iceberg) | SCD2, `check` strategy on `vendor_name`, source is the `vendors` seed |
| `fct_trips` | table (hive, parquet) | One row per trip; joins zones and the current vendor row (`dbt_valid_to is null`); adds duration, speed, tip percent, payment name |
| `mart_daily_zone_revenue` | table (hive, parquet) | Trips and revenue per pickup zone per day |

Cost control: `min_year` and `max_year` vars (both 2024 in
`dbt_project.yml`) restrict which partitions staging scans. Override with
`--vars '{min_year: 2023, max_year: 2024}'`.

## Data quality

dbt tests (`data_tests` in the yml files):

- `stg_zones.zone_id`: not_null, unique.
- `stg_trips.trip_id`: not_null, unique. `vendor_id`: not_null, accepted values 1, 2, 6, 7. `payment_type`: accepted values 0 to 6. `pickup_at`: not_null.
- `dim_zone.zone_id`: not_null, unique.
- `fct_trips.trip_id`: not_null, unique. `vendor_id` and `pickup_zone_id`: not_null and relationships to `dim_vendor` and `dim_zone`. `dropoff_zone_id`: relationships to `dim_zone`. `payment_type`: accepted values. `trip_duration_min`: not_null.
- `mart_daily_zone_revenue.pickup_date`: not_null.
- `dim_vendor.vendor_id` and `vendor_name`: not_null.
- Singular test `assert_dropoff_after_pickup`: no `fct_trips` row with dropoff before pickup.

Row filters in `stg_trips` (rows removed, not flagged): null pickup, dropoff
before pickup, duration of 24 hours or more, negative distance or total, and
pickup outside the file's own year and month.

Ingestion checks: a downloaded trip file must contain 10 required columns
(read from the parquet footer) before upload.

Python tests: `tests/ingestion/test_pipeline.py` (14 tests) and
`tests/dags/test_dag_import.py`.

## CI

`.github/workflows/ci.yml` runs on pull requests with no AWS credentials:

- `lint-test`: `ruff check .` and pytest excluding `tests/dags`.
- `dag-tests`: installs Airflow 2.10.5 with its constraint file and runs `tests/dags`.
- `dbt-parse`: `dbt deps` and `dbt parse` only. Models are never compiled or run against Athena in CI.

## Limitations

- The Streamlit app does not exist in this branch. `docker/streamlit.Dockerfile` is a stub and the compose service is commented out.
- CI does not run `dbt build` or `dbt test`. Model SQL and tests are only verified by a manual run against AWS.
- The DAG has no `dbt seed` task and no `create_raw_tables` task. `dim_vendor` selects from the `vendors` seed, so run `dbt seed` and `create_raw_tables` once before the first DAG run.
- The DAG starts at 2025-01-01 with `catchup=True`, but `min_year`/`max_year` are 2024. Months ingested for 2025 are not read by staging until the vars change.
- `dbt_build` rebuilds the marts and views over the whole configured year range on every monthly run. Only ingestion is per month.
- Ingestion skip logic compares object size only (see ADR 0005).
- `trip_id` is a hash of six fields. Two real trips with identical values collapse into one row.
- Partitions for older years may have different physical column types and can fail on read (noted in the `create_raw_tables` macro).
- Single environment, no staging/prod split, no alerting beyond Airflow retries (3 retries, exponential backoff, 5 minute delay).
- `download.py` and `soda.py` write to local `data/raw` and are not part of the DAG.
- Default bucket name and profile are hard-coded in `ingestion/pipeline.py` and `dbt/profiles.yml`; override with `S3_BUCKET`, `AWS_PROFILE`, `DBT_S3_STAGING_DIR`, `DBT_S3_DATA_DIR`.

## Interview talking points

- Story: monthly files land in S3 untouched, so the raw layer can be replayed. Transformations are SQL in dbt with tests, run by a scheduler with retries, the same shape as a warehouse ELT at work.
- Idempotency: re-running a month is safe because ingestion skips files whose S3 size equals the source `Content-Length`, and dbt models are rebuilt from raw. Known weakness: equal size with different content is not detected (ADR 0005).
- SCD2 without a warehouse feature: `dbt snapshot` with the `check` strategy gives history on vendor names. Athena needs Iceberg for the MERGE this requires (ADR 0003).
- Cost: Athena bills per data scanned. Partition projection and the `year` predicate in `stg_trips` limit the scan to the configured years (ADR 0002).
- Snowflake migration: the models use Athena/Trino SQL functions (`date_diff`, `to_hex`, `md5(to_utf8(...))`, `day_of_week`). Moving to Snowflake means a new `dbt-snowflake` profile in `profiles.yml`, replacing those functions (for example with `datediff`, `md5`, `dayofweek`), removing the hive/Iceberg `table_type` configs, and replacing the Glue external tables with Snowflake external tables or `COPY INTO`. The model structure, tests and DAG stay. This is an estimate; no Snowflake run was done.
- Trade-offs I can discuss: size-based skip versus checksums, CI without secrets versus real integration tests, and why the DAG does not seed or create tables.
