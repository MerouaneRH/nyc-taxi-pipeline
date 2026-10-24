"""Idempotent TLC -> S3 raw-zone loader.

Usage:
    uv run python -m ingestion.pipeline --service yellow --months 2024-01 2024-02
    uv run python -m ingestion.pipeline --zones

Env: AWS_PROFILE (default nyc-taxi), S3_BUCKET (default below), region eu-west-3.
"""

import argparse
import csv
import os
import sys
import tempfile
from pathlib import Path

import boto3
import pyarrow.parquet as pq
import requests
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import BotoCoreError, ClientError
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestion.download import BASE_URL, trip_url

REGION = "eu-west-3"
DEFAULT_BUCKET = "merouane-nyc-taxi-raw-euw3-2026"
DEFAULT_PROFILE = "nyc-taxi"
ZONES_KEY = "raw/zones/taxi_zone_lookup.csv"
ZONES_URL = f"{BASE_URL}/misc/taxi_zone_lookup.csv"
MAX_ATTEMPTS = 5
CHUNK = 1 << 20
HEADERS = {"Accept-Encoding": "identity"}

YELLOW_REQUIRED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "total_amount",
]
REQUIRED_COLUMNS = {"yellow": YELLOW_REQUIRED_COLUMNS}
ZONES_COLUMNS = ["LocationID", "Borough", "Zone", "service_zone"]
DEFAULT_MAX_BYTES = 2 * 1024**3


class SchemaError(ValueError):
    """Parquet file is missing required columns."""


class DownloadError(ValueError):
    """Download refused: redirect, oversize, or length mismatch. Not retried."""


def max_download_bytes() -> int:
    return int(os.environ.get("MAX_DOWNLOAD_BYTES", DEFAULT_MAX_BYTES))


def _refuse_redirect(resp: requests.Response) -> None:
    if resp.is_redirect or 300 <= resp.status_code < 400:
        raise DownloadError(f"refusing redirect ({resp.status_code}) from {resp.url}")


def _check_declared(resp: requests.Response) -> int | None:
    length = resp.headers.get("Content-Length")
    if length is not None and int(length) > max_download_bytes():
        raise DownloadError(
            f"Content-Length {length} exceeds cap {max_download_bytes()}"
        )
    return int(length) if length is not None else None


def bucket_name() -> str:
    return os.environ.get("S3_BUCKET", DEFAULT_BUCKET)


def make_s3():
    session = boto3.Session(
        profile_name=os.environ.get("AWS_PROFILE", DEFAULT_PROFILE), region_name=REGION
    )
    return session.client("s3")


def month_key(service: str, month: str) -> str:
    year, mm = month.split("-")
    return f"raw/{service}/year={year}/month={mm}/{service}_tripdata_{month}.parquet"


def _retryable_http(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code
        return status >= 500 or status == 429
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


def _retryable_s3(exc: BaseException) -> bool:
    if isinstance(exc, ClientError):
        return exc.response["Error"]["Code"] not in {"404", "NoSuchKey", "NotFound"}
    return isinstance(exc, (BotoCoreError, S3UploadFailedError))


def _backoff(check):
    return retry(
        retry=retry_if_exception(check),
        stop=stop_after_attempt(MAX_ATTEMPTS),
        wait=wait_exponential(multiplier=1, max=30),
        reraise=True,
    )


@_backoff(_retryable_http)
def remote_size(url: str) -> int | None:
    resp = requests.head(url, headers=HEADERS, timeout=30, allow_redirects=False)
    _refuse_redirect(resp)
    resp.raise_for_status()
    length = resp.headers.get("Content-Length")
    return int(length) if length else None


def _write_capped(resp: requests.Response, dest: Path, declared: int | None) -> None:
    cap, total = max_download_bytes(), 0
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=CHUNK):
            total += len(chunk)
            if total > cap:
                raise DownloadError(f"download exceeds cap {cap} bytes")
            if declared is not None and total > declared:
                raise DownloadError(
                    f"download exceeds declared Content-Length {declared}"
                )
            f.write(chunk)


@_backoff(_retryable_http)
def stream_to_file(url: str, dest: Path) -> None:
    """Stream url to dest in chunks; no redirects, size-capped, partial file removed on failure."""
    with requests.get(
        url, headers=HEADERS, stream=True, timeout=60, allow_redirects=False
    ) as resp:
        _refuse_redirect(resp)
        resp.raise_for_status()
        declared = _check_declared(resp)
        try:
            _write_capped(resp, dest, declared)
        except BaseException:
            dest.unlink(missing_ok=True)
            raise


@_backoff(_retryable_s3)
def s3_size(s3, bucket: str, key: str) -> int | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


@_backoff(_retryable_s3)
def upload(s3, path: Path, bucket: str, key: str) -> None:
    s3.upload_file(str(path), bucket, key)  # multipart above threshold


def validate_parquet(path: Path, service: str) -> None:
    """Check required columns from the footer only; no row data is read."""
    names = set(pq.read_schema(path).names)
    missing = [c for c in REQUIRED_COLUMNS[service] if c not in names]
    if missing:
        raise SchemaError(f"{path.name} missing columns: {', '.join(missing)}")


def validate_zones_csv(path: Path) -> None:
    """Check the header row has exactly the expected columns."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        header = next(csv.reader(f), [])
    if header != ZONES_COLUMNS:
        missing = [c for c in ZONES_COLUMNS if c not in header]
        raise SchemaError(
            f"{path.name} header {header} != {ZONES_COLUMNS}; missing: {', '.join(missing)}"
        )


def _is_current(s3, bucket: str, key: str, url: str) -> bool:
    have = s3_size(s3, bucket, key)
    if have is None:
        return False
    try:
        want = remote_size(url)
    except requests.RequestException:
        want = None
    return want is None or want == have


def _sync(s3, bucket: str, key: str, url: str, validate) -> str:
    if _is_current(s3, bucket, key, url):
        return "skipped"
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / Path(key).name
        stream_to_file(url, path)
        validate(path)
        upload(s3, path, bucket, key)
    return "uploaded"


def ingest_month(s3, bucket: str, service: str, month: str) -> str:
    return _sync(
        s3,
        bucket,
        month_key(service, month),
        trip_url(service, month),
        lambda p: validate_parquet(p, service),
    )


def ingest_zones(s3, bucket: str) -> str:
    return _sync(s3, bucket, ZONES_KEY, ZONES_URL, validate_zones_csv)


def _month(value: str) -> str:
    trip_url("yellow", value)  # reuses format validation
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--service", choices=sorted(REQUIRED_COLUMNS), default="yellow")
    parser.add_argument("--months", nargs="+", default=[], type=_month)
    parser.add_argument(
        "--zones", action="store_true", help="also load taxi_zone_lookup.csv"
    )
    args = parser.parse_args(argv)

    s3, bucket = make_s3(), bucket_name()
    if args.zones:
        print(f"{ingest_zones(s3, bucket):9} s3://{bucket}/{ZONES_KEY}")
    for month in args.months:
        result = ingest_month(s3, bucket, args.service, month)
        print(f"{result:9} s3://{bucket}/{month_key(args.service, month)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
