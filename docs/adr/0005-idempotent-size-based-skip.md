# 0005. Idempotent ingestion with a size-based skip

Status: Accepted

## Context and problem

Airflow retries and reruns must not download a file twice or leave duplicates. A monthly TLC file is tens of megabytes.

## Options

- Always overwrite.
- Skip if the S3 key exists.
- Skip if the S3 object size equals the source `Content-Length`.
- Compare checksums.

## Decision

`ingestion/pipeline.py` writes each month to a fixed key (`raw/yellow/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet`). It reads the S3 size with `head_object` and the source size with an HTTP HEAD. It skips the file if the sizes match, or if the source size cannot be read. Otherwise it streams the file to a temporary directory, checks the required columns in the parquet footer, and uploads. HTTP and S3 calls retry up to 5 times with exponential backoff.

The download also refuses redirects, stops above `MAX_DOWNLOAD_BYTES` (2 GiB by default), and fails when the byte count differs from `Content-Length`.

## Consequences

- Rerunning a month is cheap and leaves the same S3 state.
- A partial file never appears under the final key, because the upload starts after the full download.
- Weakness: if TLC republishes a file with new content and the same byte size, nobody notices. A checksum or `Last-Modified` comparison would fix this.
- Weakness: if the HEAD request fails, an existing object is kept without a check.
- Weakness: the schema check covers 10 column names, not their types.
- `ingest_zones` uses the same rule for the zone CSV. Its check compares the header row with `LocationID,Borough,Zone,service_zone`.
