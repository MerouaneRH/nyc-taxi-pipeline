# Operating runbook

Region `eu-west-3`. Default bucket `merouane-nyc-taxi-raw-euw3-2026` (override with `S3_BUCKET`). Default AWS profile `nyc-taxi` (override with `AWS_PROFILE`). Commands run from the repo root unless noted.

## First-time setup

1. Create the bucket and an AWS profile with the permissions in "IAM" below.
2. `uv sync --group dbt`
3. Load zones and a month of trips (see "Run a month").
4. From `dbt/`, once:
   ```bash
   dbt deps --profiles-dir . --target athena
   dbt run-operation create_raw_tables --profiles-dir . --target athena
   dbt seed --profiles-dir . --target athena
   ```
   `create_raw_tables` is idempotent. It creates Glue database `nyc_taxi_raw` and the tables `yellow_trips` and `taxi_zone_lookup`. The DAG task `dbt_prepare` runs both `create_raw_tables` and `dbt seed` on every run, so this manual step is only needed for manual runs.
5. Create the Athena workgroup once: `AWS_PROFILE=nyc-taxi uv run python dbt/scripts/create_workgroup.py` (see "Athena workgroup" below).

## Run a month (manual)

```bash
uv run python -m ingestion.pipeline --zones
uv run python -m ingestion.pipeline --service yellow --months 2024-03
cd dbt
dbt snapshot --profiles-dir . --target athena
dbt build    --profiles-dir . --target athena
```

Incremental month (ADR 0007). `fct_trips` is partitioned by month. To process only one month:

```bash
dbt build --select 'stg_zones+ stg_trips+' --profiles-dir . --target athena --vars '{run_month: "2024-03"}'
```

`run_month` must be `YYYY-MM` inside `min_year..max_year`, else compilation fails. Without `run_month`, all partitions in range are rewritten. After a change to `fct_trips` columns or the first run, use `dbt build --full-refresh`.

Athena workgroup (ADR 0007): the workgroup `nyc-taxi-dbt` exists, with a 5 GiB per-query scan cap that is enforced. dbt uses it (`DBT_ATHENA_WORKGROUP`); the app reads `ATHENA_WORKGROUP`. `dbt/scripts/create_workgroup.py` creates it (idempotent).

Output per file is `uploaded` or `skipped`. `skipped` means the S3 size equals the source size (ADR 0005).

Staging reads only `min_year` to `max_year` (2024 to 2024 by default). For another year:

```bash
dbt build --profiles-dir . --target athena --vars '{min_year: 2025, max_year: 2025}'
```

## Run through Airflow

```bash
cp .env.example .env
# set POSTGRES_PASSWORD, AIRFLOW_ADMIN_PASSWORD, AIRFLOW__CORE__FERNET_KEY, AIRFLOW__WEBSERVER__SECRET_KEY
mkdir -p .aws-airflow    # copy only the nyc-taxi profile from ~/.aws into it (credentials + config); gitignored
docker compose up --build -d
```

Open `http://localhost:8080` (bound to `127.0.0.1` only), log in with `AIRFLOW_ADMIN_USER` (default `admin`), unpause `nyc_taxi_elt`. Task order: `ingest_zones`, `ingest_trips`, `dbt_prepare`, `dbt_snapshot`, `dbt_build`, `notify_new`. Settings: schedule `0 */6 * * *` (every 6 hours, ADR 0010), `start_date` 2024-01-01, `catchup=False`, `max_active_runs=1`, 3 retries with exponential backoff from 5 minutes.

Each run discovers months (ADR 0009): `ingest_trips` runs `ingestion.pipeline --since <ingest_since>` and checks every month from `ingest_since` (default `2024-01`) through the current month. `dbt_snapshot` takes `min_year` from the year of `ingest_since` and `max_year` from the run's year, so a backfill is covered. `dbt_build` builds each month that `ingest_trips` uploaded in this run (oldest first), with `--select stg_zones+ stg_trips+`, that month's own year as `min_year`/`max_year`, and `run_month` set to the month.

Late and unpublished months: `ingest_trips` runs with `--skip-unpublished` and `skip_on_exit_code=99`. Months TLC has not published (CloudFront returns 403) are logged and skipped. If nothing was uploaded in the run (every month current or unpublished), the task exits 99 and is skipped, and so are `dbt_prepare`, `dbt_snapshot` and `dbt_build`. The task is not failed. See ADR 0009.

### What discovery does

For each month from the start month through the current UTC month, the loader compares the S3 object size with the TLC file size (two cheap HEAD requests). A month missing in S3 or with a different size is downloaded, validated and uploaded. A month TLC has not published (HTTP 403) is logged as `unpublished` and skipped; a later run picks it up. The last stdout line is `NEW_MONTHS=<uploaded months, oldest first>`; exit 99 means nothing was uploaded and dbt is skipped.

### Preview with a dry run

```bash
AWS_PROFILE=nyc-taxi uv run python -m ingestion.pipeline --since 2024-01 --service yellow --skip-unpublished --dry-run
```

Prints one line per month (`would-upload`, `current` or `unpublished`), then `NEW_MONTHS=` and exits with the code a real run would. Nothing is downloaded or uploaded. Run this before the first run after deploy to see how many months a backfill will load.

### Change the start month

The start month is the DAG param `ingest_since` (`YYYY-MM`, must not be after the run month; a bad value fails the `ingest_trips` task). For one run:

```bash
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_elt \
  --conf '{"ingest_since": "2025-01"}'
```

To change the default, edit the `Param("2024-01", ...)` in `dags/nyc_taxi_elt.py` and restart the scheduler. The `--conf` form was not run here; check the rendered `--since` value in the `ingest_trips` log. Months before the start month are never checked.

### Load a year in a batch

`ingest_until` (`YYYY-MM`, inclusive, default empty = current month) bounds the range with `ingest_since`. It must be `>= ingest_since` and well formed, otherwise the `ingest_trips` task fails at render time. To load 2024 in one run:

```bash
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_elt \
  --conf '{"ingest_since": "2024-01", "ingest_until": "2024-12"}'
```

The 6-hourly runs that follow use the defaults again (no `--until`). This `--conf` form was not run here; check the rendered `--since`/`--until` in the `ingest_trips` log.

## Backfill

Use the batch trigger above. Without Airflow, loop over months:

```bash
uv run python -m ingestion.pipeline --service yellow --months 2024-01 2024-02 2024-03
```

Then run `dbt snapshot` and `dbt build` once. A backfill into years outside `min_year`/`max_year` is not visible in the models until the vars are widened. Widening the range increases bytes scanned.

## Rerun a failed task

In the Airflow UI, open the failed task instance and select Clear. Downstream tasks rerun. From the CLI:

```bash
docker compose exec airflow-scheduler airflow tasks clear nyc_taxi_elt \
  --task-regex dbt_build --start-date 2025-04-01 --end-date 2025-04-01 --yes
```

Reruns are safe: ingestion skips files that match the source size, and the month's `fct_trips` partition is overwritten. To force a re-download, delete the S3 object first:

```bash
aws s3 rm s3://$S3_BUCKET/raw/yellow/year=2024/month=03/yellow_tripdata_2024-03.parquet --profile nyc-taxi
```

Run a single model: `dbt build --select fct_trips+ --profiles-dir . --target athena`.

## Rotate credentials

Credentials are never in the repo. The containers read `./.aws-airflow` (gitignored, mounted read-only at `/home/airflow/.aws`), which holds only the `nyc-taxi` profile. `AWS_PROFILE` is fixed to `nyc-taxi` in compose.

1. Create a new access key for the IAM user, update `~/.aws/credentials` and the copy in `.aws-airflow/`, then deactivate and delete the old key.
2. Verify: `aws sts get-caller-identity --profile nyc-taxi`.
3. Restart services so no task holds old credentials: `docker compose restart airflow-scheduler airflow-webserver`.

For the database and admin passwords, change `.env`. Postgres keeps the old password in the `postgres-db` volume, so change it inside Postgres too or recreate the volume (`docker compose down -v` deletes Airflow metadata).

## Cost control

- Athena bills per bytes scanned. Keep `min_year`/`max_year` narrow.
- Staging filters on partition column `year`, and the raw table uses partition projection, so unscanned partitions cost nothing.
- dbt runs in workgroup `nyc-taxi-dbt` (5 GiB per-query scan cap, enforced). Keep `DBT_ATHENA_WORKGROUP` pointed at it.
- Marts are tables, so Streamlit queries hit the small mart and not raw data.
- `athena-results/` accumulates query output. Add an S3 lifecycle rule to expire it.
- Stop local services when idle: `docker compose down`. Nothing else in this repo runs continuously.
- Check spend in AWS Cost Explorer. The repo has no budget alarm.

## IAM

The two errors in the table below were seen in practice. The rest of this list is a typical set for this setup and was not checked action by action. The profile needs, on the bucket and prefixes used here:

- S3: `GetObject`, `PutObject`, `ListBucket`, `GetBucketLocation` on the bucket; `PutObject` and `DeleteObject` under `warehouse/` and `athena-results/`.
- Athena: `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`, `StopQueryExecution`, `GetWorkGroup`. Creating the workgroup (`dbt/scripts/create_workgroup.py`) also needs `CreateWorkGroup` (and `UpdateWorkGroup` for reruns of the script).
- Glue: database and table create/read/update/delete on `nyc_taxi_raw` and `nyc_taxi_dbt`, including `GetTableVersions`, `GetPartitions`, `BatchCreatePartition`.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `AccessDenied ... s3:PutObject ... warehouse/` during `dbt run` or `dbt snapshot` | The role cannot write under `warehouse/` (`DBT_S3_DATA_DIR`). Grant `s3:PutObject` (and `DeleteObject`) on `arn:aws:s3:::<bucket>/warehouse/*`. Iceberg snapshot writes need it too. |
| `AccessDeniedException ... glue:GetTableVersions` | Iceberg and table replace read Glue table versions. Add `glue:GetTableVersions` (and `GetTableVersion`) on the catalog, database and tables. |
| `Table ... not found` for `yellow_trips` or `taxi_zone_lookup` | Run `dbt run-operation create_raw_tables`. |
| `Relation ... vendors does not exist` in `dbt snapshot` | Run `dbt seed` first (the DAG does this in `dbt_prepare`). |
| Athena `HIVE_BAD_DATA` / type mismatch on a month | That file has different physical column types. Narrow `min_year`/`max_year` to years with the 2024 schema. |
| Zero rows in `fct_trips` | Month not ingested, or the year is outside the vars. Check S3 keys and `--vars`. |
| `SchemaError: ... missing columns` | The downloaded file lacks one of the 10 required columns. The file is not uploaded. Inspect the source file. |
| `403`/`404` from `remote_size` / download | The month is not published yet (CloudFront returns 403). In the DAG this is a skip for that month; by hand, pass `--skip-unpublished` to log it and carry on (exit code 99 only if no month was uploaded). Check `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`. |
| `docker compose` fails with `set in .env` | Set `POSTGRES_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW__CORE__FERNET_KEY` and `AIRFLOW__WEBSERVER__SECRET_KEY` in `.env`. |
| DAG not listed | Check `docker compose exec airflow-scheduler airflow dags list-import-errors`. |
| Task cannot find credentials in container | Check `./.aws-airflow` holds the `nyc-taxi` profile (credentials and config). |
| `dbt test` fails on `accepted_values` for `vendor_id` | A new vendor ID appeared. Add it to `dbt/seeds/vendors.csv` and to the test list in `dbt/models/staging/staging.yml`. |
