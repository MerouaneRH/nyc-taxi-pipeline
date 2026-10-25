# Runbook

Task-oriented steps for running and fixing the pipeline. Defaults: region
`eu-west-3`, bucket `merouane-nyc-taxi-raw-euw3-2026` (override with
`S3_BUCKET`), AWS profile `nyc-taxi` (override with `AWS_PROFILE`).

Python commands run from the repo root. dbt commands run from `dbt/` and use
`--profiles-dir . --target athena`. Airflow commands run through
`docker compose exec airflow-scheduler`.

## Set up for the first time

1. Create the S3 bucket and an AWS profile with the permissions in [IAM permissions](#iam-permissions).
2. Install: `uv sync --group dbt`.
3. Create the Athena workgroup that caps scan cost (safe to repeat):
   ```bash
   AWS_PROFILE=nyc-taxi uv run python dbt/scripts/create_workgroup.py
   ```
4. Load the zone file and one month (see [Load a new month](#load-a-new-month)).
5. Create the Glue tables and the vendors seed. The DAG task `dbt_prepare` does both on every run, so you only need this for manual runs.
   ```bash
   cd dbt
   uv run --group dbt dbt deps --profiles-dir . --target athena
   uv run --group dbt dbt run-operation create_raw_tables --profiles-dir . --target athena
   uv run --group dbt dbt seed --profiles-dir . --target athena
   ```
   `create_raw_tables` creates the Glue database `nyc_taxi_raw` with `yellow_trips` and `taxi_zone_lookup`.

## Run Airflow locally

```bash
cp .env.example .env
# set POSTGRES_PASSWORD, AIRFLOW_ADMIN_PASSWORD, AIRFLOW__CORE__FERNET_KEY, AIRFLOW__WEBSERVER__SECRET_KEY
mkdir -p .aws-airflow    # copy only the nyc-taxi profile from ~/.aws into it; the folder is git-ignored
docker compose up --build -d
```

Open `http://localhost:8080` (bound to `127.0.0.1`). Log in as
`AIRFLOW_ADMIN_USER` (default `admin`) and unpause `nyc_taxi_elt`. The DAG
runs every 6 hours. Stop everything with `docker compose down`.

## Load a new month

Nothing to do in normal operation. Every 6 hours the DAG checks each month from
`ingest_since` through the current month, loads any month that TLC has
published and S3 lacks, and builds it. TLC publishes about two months after a
month ends.

To check now without waiting, trigger a run:

```bash
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_elt
```

To preview what a run would load, without downloading or uploading anything:

```bash
AWS_PROFILE=nyc-taxi uv run python -m ingestion.pipeline --since 2024-01 --service yellow --skip-unpublished --dry-run
```

It prints `would-upload`, `current` or `unpublished` for each month, then
`NEW_MONTHS=`, and exits with the code a real run would give.

To load a month by hand, without Airflow:

```bash
uv run python -m ingestion.pipeline --zones
uv run python -m ingestion.pipeline --service yellow --months 2024-03
cd dbt
uv run --group dbt dbt snapshot --profiles-dir . --target athena
uv run --group dbt dbt build --select 'stg_zones+ stg_trips+' --profiles-dir . --target athena --vars '{run_month: "2024-03"}'
```

Each file prints `uploaded` or `skipped`. `skipped` means the S3 size equals
the TLC size ([ADR 0005](adr/0005-idempotent-size-based-skip.md)).

## Load a year in a batch

Set both ends of the range with the DAG params `ingest_since` and
`ingest_until` (`YYYY-MM`, inclusive). `ingest_until` must not be before
`ingest_since`. A bad value fails `ingest_trips` when the DAG renders it. To
load all of 2024 in one run:

```bash
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_elt \
  --conf '{"ingest_since": "2024-01", "ingest_until": "2024-12"}'
```

The 6-hourly runs after that use the defaults again (no end month). `dbt_build`
builds one month at a time, about 2 minutes each, so a year takes a while. This
`--conf` form has not been tried against a live Airflow. Check the `--since` and
`--until` values in the `ingest_trips` log.

Without Airflow, list the months:

```bash
uv run python -m ingestion.pipeline --service yellow --months 2024-01 2024-02 2024-03
```

Then run `dbt snapshot` and `dbt build` once. Months outside `min_year` to
`max_year` are invisible to the models until you widen those variables, and
widening them raises the bytes scanned.

## Change the start month

The start month is the DAG param `ingest_since` (`YYYY-MM`, default `2024-01`).
It must not be after the run month. Months before it are never checked. For one
run:

```bash
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_elt \
  --conf '{"ingest_since": "2025-01"}'
```

To change the default, edit `Param("2024-01", ...)` in `dags/nyc_taxi_elt.py`
and restart the scheduler. A later start month makes every run cheaper, because
each run makes two requests per month since the start.

## Rebuild one month

Use this after you change a model or a threshold. It rewrites that month's
partition in each incremental model and rebuilds the plain tables.

```bash
cd dbt
uv run --group dbt dbt build --select 'stg_zones+ stg_trips+' --profiles-dir . --target athena \
  --vars '{run_month: "2024-03"}'
```

`run_month` must be `YYYY-MM` and inside `min_year` to `max_year`, or
compilation fails. The defaults are 2024 to 2024. For another year, set them:

```bash
uv run --group dbt dbt build --select 'stg_zones+ stg_trips+' --profiles-dir . --target athena \
  --vars '{min_year: 2024, max_year: 2025, run_month: "2025-03"}'
```

Without `run_month`, dbt rewrites every partition in range. After a change to
the columns of an incremental model, add `--full-refresh`, which drops and
rebuilds the table. To run one model and what depends on it:
`dbt build --select fct_trips+ ...`.

To re-download a month as well, delete its S3 object, then trigger the DAG. The
loader sees the month as missing and builds it:

```bash
aws s3 rm s3://$S3_BUCKET/raw/yellow/year=2024/month=03/yellow_tripdata_2024-03.parquet --profile nyc-taxi
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_elt
```

## Check a failed run

1. Open the DAG in the Airflow UI, select the failed task, and open its log. Skipped tasks are not failures. A run where `ingest_trips` and the dbt tasks are all skipped means nothing was new (exit code 99).
2. List recent runs from the CLI:
   ```bash
   docker compose exec airflow-scheduler airflow dags list-runs -d nyc_taxi_elt
   ```
3. Read the log for these lines:
   - `ingest_trips`: one `uploaded`, `skipped` or `unpublished` line per month, then `NEW_MONTHS=...`.
   - `dbt_build`: one dbt build per new month. The first failed model or test ends the loop.
4. Look up the error in [Troubleshooting](#troubleshooting).
5. Fix the cause, then clear the failed task. In the UI, select Clear. Downstream tasks rerun. From the CLI:
   ```bash
   docker compose exec airflow-scheduler airflow tasks clear nyc_taxi_elt \
     --task-regex dbt_build --start-date 2025-04-01 --end-date 2025-04-01 --yes
   ```

Reruns are safe. Ingestion skips files whose size matches, and each month's
partitions are overwritten. If the DAG does not appear, run
`docker compose exec airflow-scheduler airflow dags list-import-errors`.

## Read the data-quality page

Open the Data quality view in the dashboard. It works in whole months, so it
widens the date range you picked.

1. Start with the four tiles: raw rows, staged rows, the percentage dropped, and suspect trips. Dropped rows were removed by staging. Suspect trips are still in the data.
2. "What was removed" counts raw rows by drop rule. One row can break several rules, so the counts can add up to more than the dropped total. Negative totals are refunds and adjustments, and the sentence under the table gives their dollar sum.
3. "Suspect trips by reason" shows, for each reason code, the trip count, the share of staged rows, the money on those trips and a plain explanation. The rules and thresholds are in [data-model.md](data-model.md#suspect-trip-rules).
4. Under "Drill down", pick a reason code, and optionally a vendor and a payment type. The table lists up to 200 trips, most extreme first, and a button downloads them as CSV. Compare duration, distance and speed in a row. The one that is impossible is the likely broken field.
5. The distribution chart below uses a log-like scale so rare extreme buckets stay visible. The vendor and payment filters do not apply to it.

## Change a suspect-trip threshold

The thresholds are the `dq_*` variables in `dbt/dbt_project.yml`. Edit one, then
rebuild each affected month with [Rebuild one month](#rebuild-one-month). Only
`dq_suspect_trips` and `mart_dq_summary` change.

## Add a vendor

The test `accepted_values` on `vendor_id` fails when a vendor ID appears that is
not 1, 2, 6 or 7. Add the vendor to `dbt/seeds/vendors.csv` and to the value list
in `dbt/models/staging/staging.yml`. Then run `dbt seed` and `dbt snapshot`.

## Rotate credentials

No credentials are in the repo. The containers read `./.aws-airflow` (git-ignored,
mounted read-only at `/home/airflow/.aws`), which holds only the `nyc-taxi`
profile. Compose fixes `AWS_PROFILE` to `nyc-taxi`.

1. Create a new access key for the IAM user. Update `~/.aws/credentials` and the copy in `.aws-airflow/`. Then deactivate and delete the old key.
2. Check it: `aws sts get-caller-identity --profile nyc-taxi`.
3. Restart the services so no task keeps the old key: `docker compose restart airflow-scheduler airflow-webserver`.

For the database and admin passwords, change `.env`. Postgres keeps the old
password in the `postgres-db` volume, so change it inside Postgres too, or
recreate the volume. `docker compose down -v` deletes the Airflow metadata.

## Control cost

- Athena bills by bytes scanned. Keep `min_year` and `max_year` narrow.
- The raw table uses partition projection and staging filters on `year`, so folders outside the range cost nothing.
- dbt runs in the workgroup `nyc-taxi-dbt`, which fails any query above 5 GiB scanned. Keep `DBT_ATHENA_WORKGROUP` pointed at it. The app reads `ATHENA_WORKGROUP`.
- The dashboard reads the small marts, not the raw data.
- `athena-results/` grows with every query. Add an S3 lifecycle rule to expire it.
- Stop idle local services with `docker compose down`. Nothing else in this repo runs on its own.
- Check spend in AWS Cost Explorer. The repo has no budget alarm.

## IAM permissions

The two errors marked in the troubleshooting table were seen in practice. The
rest of this list is a typical set for this setup and was not checked action by
action. The profile needs, on the bucket and prefixes used here:

- S3: `GetObject`, `PutObject`, `ListBucket` and `GetBucketLocation` on the bucket; `PutObject` and `DeleteObject` under `warehouse/` and `athena-results/`.
- Athena: `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`, `StopQueryExecution`, `GetWorkGroup`. `dbt/scripts/create_workgroup.py` also needs `CreateWorkGroup`, and `UpdateWorkGroup` when you rerun it.
- Glue: create, read, update and delete on the databases `nyc_taxi_raw` and `nyc_taxi_dbt` and their tables, including `GetTableVersions`, `GetPartitions` and `BatchCreatePartition`.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `AccessDenied ... s3:PutObject ... warehouse/` during `dbt run` or `dbt snapshot` (seen) | The role cannot write under `warehouse/` (`DBT_S3_DATA_DIR`). Grant `s3:PutObject` and `DeleteObject` on `arn:aws:s3:::<bucket>/warehouse/*`. Iceberg snapshot writes need it too. |
| `AccessDeniedException ... glue:GetTableVersions` (seen) | Iceberg and table replace read Glue table versions. Add `glue:GetTableVersions` and `GetTableVersion` on the catalog, database and tables. |
| `Table ... not found` for `yellow_trips` or `taxi_zone_lookup` | Run `dbt run-operation create_raw_tables`. |
| `Relation ... vendors does not exist` in `dbt snapshot` | Run `dbt seed` first. The DAG does this in `dbt_prepare`. |
| Athena `HIVE_BAD_DATA` or a type mismatch on a month | That file has different column types. Narrow `min_year` and `max_year` to years with the 2024 layout. |
| Zero rows in `fct_trips` | The month is not ingested, or its year is outside the variables. Check the S3 keys and `--vars`. |
| `SchemaError: ... missing columns` | The downloaded file lacks one of the 10 required columns, so it was not uploaded. Inspect the source file. |
| `403` or `404` from `remote_size` or the download | The month is not published yet. In the DAG that month is skipped. By hand, pass `--skip-unpublished`. Check `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`. |
| `DownloadError` (redirect, over the size cap, or incomplete) | The loader refuses redirects, files above 2 GiB (`MAX_DOWNLOAD_BYTES`) and short downloads. It does not retry these. Check the URL, then rerun. |
| `docker compose` fails with `set in .env` | Set `POSTGRES_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW__CORE__FERNET_KEY` and `AIRFLOW__WEBSERVER__SECRET_KEY` in `.env`. |
| Task cannot find credentials in the container | Check that `./.aws-airflow` holds the `nyc-taxi` profile (credentials and config). |
| `dbt test` fails on `accepted_values` for `vendor_id` | A new vendor ID appeared. See [Add a vendor](#add-a-vendor). |
| `run_month ... is outside min_year..max_year` | Widen `min_year` and `max_year` in `--vars` so the month's year is inside them. |
