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
   `create_raw_tables` is idempotent. It creates Glue database `nyc_taxi_raw` and the tables `yellow_trips` and `taxi_zone_lookup`. The DAG does not run `create_raw_tables` or `dbt seed`.

## Run a month (manual)

```bash
uv run python -m ingestion.pipeline --zones
uv run python -m ingestion.pipeline --service yellow --months 2024-03
cd dbt
dbt snapshot --profiles-dir . --target athena
dbt build    --profiles-dir . --target athena
```

Output per file is `uploaded` or `skipped`. `skipped` means the S3 size equals the source size (ADR 0005).

Staging reads only `min_year` to `max_year` (2024 to 2024 by default). For another year:

```bash
dbt build --profiles-dir . --target athena --vars '{min_year: 2025, max_year: 2025}'
```

## Run through Airflow

```bash
cp .env.example .env     # set POSTGRES_PASSWORD, AIRFLOW_ADMIN_PASSWORD
docker compose up --build -d
```

Open `http://localhost:8080`, log in with `AIRFLOW_ADMIN_USER` (default `admin`), unpause `nyc_taxi_elt`. Task order: `ingest_zones`, `ingest_trips`, `dbt_snapshot`, `dbt_build`. The month comes from `data_interval_start`. Settings: `@monthly`, `start_date` 2025-01-01, `catchup=True`, `max_active_runs=1`, 3 retries with exponential backoff from 5 minutes.

Trigger one month by hand:

```bash
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_elt \
  --exec-date 2025-03-01T00:00:00+00:00
```

## Backfill

Airflow, for a date range (one run at a time because of `max_active_runs=1`):

```bash
docker compose exec airflow-scheduler airflow dags backfill nyc_taxi_elt \
  --start-date 2025-01-01 --end-date 2025-06-01
```

Without Airflow, loop over months:

```bash
uv run python -m ingestion.pipeline --service yellow --months 2024-01 2024-02 2024-03
```

Then run `dbt snapshot` and `dbt build` once. A backfill into years outside `min_year`/`max_year` is not visible in the models until the vars are widened. Widening the range increases bytes scanned.

## Rerun a failed task

In the Airflow UI, open the failed task instance and select Clear. Downstream tasks rerun. From the CLI:

```bash
docker compose exec airflow-scheduler airflow tasks clear nyc_taxi_elt \
  --task-regex dbt_build --start-date 2025-03-01 --end-date 2025-03-01 --yes
```

Reruns are safe: ingestion skips files that match the source size, and dbt models are rebuilt. To force a re-download, delete the S3 object first:

```bash
aws s3 rm s3://$S3_BUCKET/raw/yellow/year=2024/month=03/yellow_tripdata_2024-03.parquet --profile nyc-taxi
```

Run a single model: `dbt build --select fct_trips+ --profiles-dir . --target athena`.

## Rotate credentials

Credentials are never in the repo. The containers read `~/.aws` (mounted read-only) using profile `AWS_PROFILE`.

1. Create a new access key for the IAM user, update `~/.aws/credentials`, then deactivate and delete the old key.
2. Verify: `aws sts get-caller-identity --profile nyc-taxi`.
3. Restart services so no task holds old credentials: `docker compose restart airflow-scheduler airflow-webserver`.

Compose sets `AWS_PROFILE` to `default` when unset. Export `AWS_PROFILE=nyc-taxi` or set it in `.env`.

For the database and admin passwords, change `.env`. Postgres keeps the old password in the `postgres-db` volume, so change it inside Postgres too or recreate the volume (`docker compose down -v` deletes Airflow metadata).

## Cost control

- Athena bills per bytes scanned. Keep `min_year`/`max_year` narrow.
- Staging filters on partition column `year`, and the raw table uses partition projection, so unscanned partitions cost nothing.
- Use a workgroup with a per-query scan limit; set `DBT_ATHENA_WORKGROUP`.
- Marts are tables, so Streamlit queries hit the small mart and not raw data.
- `athena-results/` accumulates query output. Add an S3 lifecycle rule to expire it.
- Stop local services when idle: `docker compose down`. Nothing else in this repo runs continuously.
- Check spend in AWS Cost Explorer. The repo has no budget alarm.

## IAM

The two errors in the table below were seen in practice. The rest of this list is a typical set for this setup and was not checked action by action. The profile needs, on the bucket and prefixes used here:

- S3: `GetObject`, `PutObject`, `ListBucket`, `GetBucketLocation` on the bucket; `PutObject` and `DeleteObject` under `warehouse/` and `athena-results/`.
- Athena: `StartQueryExecution`, `GetQueryExecution`, `GetQueryResults`, `StopQueryExecution`, `GetWorkGroup`.
- Glue: database and table create/read/update/delete on `nyc_taxi_raw` and `nyc_taxi_dbt`, including `GetTableVersions`, `GetPartitions`, `BatchCreatePartition`.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `AccessDenied ... s3:PutObject ... warehouse/` during `dbt run` or `dbt snapshot` | The role cannot write under `warehouse/` (`DBT_S3_DATA_DIR`). Grant `s3:PutObject` (and `DeleteObject`) on `arn:aws:s3:::<bucket>/warehouse/*`. Iceberg snapshot writes need it too. |
| `AccessDeniedException ... glue:GetTableVersions` | Iceberg and table replace read Glue table versions. Add `glue:GetTableVersions` (and `GetTableVersion`) on the catalog, database and tables. |
| `Table ... not found` for `yellow_trips` or `taxi_zone_lookup` | Run `dbt run-operation create_raw_tables`. |
| `Relation ... vendors does not exist` in `dbt snapshot` | Run `dbt seed` first. |
| Athena `HIVE_BAD_DATA` / type mismatch on a month | That file has different physical column types. Narrow `min_year`/`max_year` to years with the 2024 schema. |
| Zero rows in `fct_trips` | Month not ingested, or the year is outside the vars. Check S3 keys and `--vars`. |
| `SchemaError: ... missing columns` | The downloaded file lacks one of the 10 required columns. The file is not uploaded. Inspect the source file. |
| `404` from `remote_size` / download | The month is not published yet. Check `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`. |
| `docker compose` fails with `set in .env` | Set `POSTGRES_PASSWORD` and `AIRFLOW_ADMIN_PASSWORD` in `.env`. |
| DAG not listed | Check `docker compose exec airflow-scheduler airflow dags list-import-errors`. |
| Task cannot find credentials in container | Check `~/.aws` exists and `AWS_PROFILE` is set. |
| `dbt test` fails on `accepted_values` for `vendor_id` | A new vendor ID appeared. Add it to `dbt/seeds/vendors.csv` and to the test list in `dbt/models/staging/staging.yml`. |
