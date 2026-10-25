"""Idempotent TLC -> S3 raw-zone loader.

Usage:
    uv run python -m ingestion.pipeline --service yellow --months 2024-01 2024-02
    uv run python -m ingestion.pipeline --service yellow --since 2024-01 --skip-unpublished
    uv run python -m ingestion.pipeline --since 2024-01 --until 2024-12 --skip-unpublished
    uv run python -m ingestion.pipeline --since 2024-01 --skip-unpublished --dry-run
    uv run python -m ingestion.pipeline --zones

Env: AWS_PROFILE (default nyc-taxi), S3_BUCKET (default below), region eu-west-3.
"""

import argparse
import csv
import os
import sys
import tempfile
from datetime import UTC, date, datetime
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
EXIT_UNPUBLISHED = 99  # contract with the DAG: BashOperator skip_on_exit_code=99
UNPUBLISHED_STATUSES = {403, 404}
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
    return isinstance(
        exc,
        (
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ContentDecodingError,
        ),
    )


def _is_unpublished(exc: requests.HTTPError) -> bool:
    resp = exc.response
    return resp is not None and resp.status_code in UNPUBLISHED_STATUSES


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
    if declared is not None and total < declared:
        raise DownloadError(f"incomplete download: got {total} of {declared} bytes")


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


def expand_since(since: str, today: date, until: str | None = None) -> list[str]:
    """Every month from `since` through `until` (inclusive) or the month of `today`.

    `until` never reaches past the month of `today`. Oldest first.
    """
    year, month = map(int, since.split("-"))
    last = (today.year, today.month)
    if until:
        last = min(last, tuple(map(int, until.split("-"))))
    months = []
    while (year, month) <= last:
        months.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--service", choices=sorted(REQUIRED_COLUMNS), default="yellow")
    which = parser.add_mutually_exclusive_group()
    which.add_argument("--months", nargs="+", default=[], type=_month)
    which.add_argument(
        "--since",
        type=_month,
        help="every month from YYYY-MM through the current UTC month (discovery)",
    )
    parser.add_argument(
        "--until",
        type=_month,
        help="with --since: last month to check (inclusive); default the current UTC month",
    )
    parser.add_argument(
        "--zones", action="store_true", help="also load taxi_zone_lookup.csv"
    )
    parser.add_argument(
        "--skip-unpublished",
        action="store_true",
        help="on HTTP 403/404 log 'unpublished <url>' and carry on instead of raising; "
        f"exit {EXIT_UNPUBLISHED} when no month was uploaded",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="only HEAD/size checks: print would-upload/current/unpublished per month, "
        "then NEW_MONTHS=; no download, no upload",
    )
    return parser


def main(argv: list[str] | None = None, today: date | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.until and not args.since:
        parser.error("--until requires --since")
    if args.until and args.until < args.since:
        parser.error(f"--until {args.until} is before --since {args.since}")
    if args.since:
        today = today or datetime.now(UTC).date()
        args.months = expand_since(args.since, today, args.until)
        if not args.months:
            parser.error(f"--since {args.since} is after the current month")
    if not args.zones and not args.months:
        parser.error("nothing to do: pass --zones and/or --months/--since")

    s3, bucket = make_s3(), bucket_name()
    zones_unpublished = False
    if args.zones:
        zones_status = _run(
            args,
            ZONES_URL,
            ZONES_KEY,
            bucket,
            lambda: ingest_zones(s3, bucket),
            label="zones",
            s3=s3,
        )
        zones_unpublished = zones_status == "unpublished"
    results = {m: _ingest_one(args, s3, bucket, m) for m in args.months}
    if not args.months:
        return EXIT_UNPUBLISHED if zones_unpublished else 0
    new = sorted(m for m, status in results.items() if status == "uploaded")
    print(f"NEW_MONTHS={' '.join(new)}")
    if args.skip_unpublished and not new:
        return EXIT_UNPUBLISHED  # nothing new for dbt to build
    return 0


def _ingest_one(args, s3, bucket: str, month: str) -> str:
    return _run(
        args,
        trip_url(args.service, month),
        month_key(args.service, month),
        bucket,
        lambda: ingest_month(s3, bucket, args.service, month),
        label=month,
        s3=s3,
    )


def _plan(s3, bucket: str, key: str, url: str) -> str:
    """Dry-run twin of _sync: same decisions, no GET and no PUT."""
    if _is_current(s3, bucket, key, url):
        return "skipped"
    remote_size(url)  # raises HTTPError 403/404 when unpublished, like the GET would
    return "uploaded"


DRY_LABELS = {"uploaded": "would-upload", "skipped": "current"}


def _run(args, url: str, key: str, bucket: str, action, label: str, s3) -> str:
    """Run one unit of work; returns 'uploaded', 'skipped' or 'unpublished'."""
    try:
        result = _plan(s3, bucket, key, url) if args.dry_run else action()
    except requests.HTTPError as exc:
        if not (args.skip_unpublished and _is_unpublished(exc)):
            raise
        if args.dry_run:
            print(f"unpublished {label}")
        else:
            print(f"unpublished {url}", file=sys.stderr)
        return "unpublished"
    if args.dry_run:
        print(f"{DRY_LABELS[result]} {label}")
    else:
        print(f"{result:9} s3://{bucket}/{key}")
    return result


if __name__ == "__main__":
    sys.exit(main())
