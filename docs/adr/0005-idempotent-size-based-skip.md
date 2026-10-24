# 0005. Idempotent ingestion with a size-based skip

Status: Accepted

## Context and problem

Airflow retries and re-runs must not re-download or duplicate files. TLC monthly files are tens of megabytes each.

## Options

- Always overwrite.
- Skip if the S3 key exists.
- Skip if the S3 object size equals the source `Content-Length`.
- Compare checksums.

## Decision

`ingestion/pipeline.py` writes to a deterministic key per month (`raw/yellow/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet`). It reads the S3 size with `head_object` and the source size with an HTTP HEAD. It skips if they match, or if the source size cannot be fetched. Otherwise it streams to a temp file, checks required columns in the parquet footer, and uploads. HTTP and S3 calls retry up to 5 times with exponential backoff.

## Consequences

- Re-running a month is cheap and leaves the same S3 state.
- A partial file never appears under the final key: the upload starts after the full download.
- Weakness: if TLC republishes a file with different content and the same byte size, the change is not detected. A checksum or `Last-Modified` comparison would fix this.
- Weakness: if the HEAD request fails, an existing object is kept without verification.
- Weakness: the schema check covers 10 column names only, not types.
- `ingest_zones` uses the same rule for the zone CSV, without the schema check.
