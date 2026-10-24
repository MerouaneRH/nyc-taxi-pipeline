import io

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests
import responses
from botocore.exceptions import ClientError
from moto import mock_aws

from ingestion import pipeline
from ingestion.download import trip_url

BUCKET = "test-bucket"
REGION = "eu-west-3"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


ZONES_CSV = (
    b'"LocationID","Borough","Zone","service_zone"\n1,"EWR","Newark Airport","EWR"\n'
)


def parquet_bytes(columns=None) -> bytes:
    columns = columns or pipeline.YELLOW_REQUIRED_COLUMNS
    table = pa.table({c: pa.array([1]) for c in columns})
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr("tenacity.nap.time.sleep", lambda s: None)


@pytest.fixture
def s3(monkeypatch):
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.setenv(k, "testing")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        client.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION}
        )
        yield client


def mock_source(body: bytes, month="2024-01"):
    url = trip_url("yellow", month)
    responses.add(responses.HEAD, url, headers={"Content-Length": str(len(body))})
    responses.add(responses.GET, url, body=body)
    return url


def list_keys(s3):
    resp = s3.list_objects_v2(Bucket=BUCKET)
    return [o["Key"] for o in resp.get("Contents", [])]


def n_gets():
    return sum(c.request.method == "GET" for c in responses.calls)


def test_key_layout():
    assert pipeline.month_key("yellow", "2024-01") == (
        "raw/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet"
    )
    assert pipeline.ZONES_KEY == "raw/zones/taxi_zone_lookup.csv"


def test_bucket_default_and_env(monkeypatch):
    monkeypatch.delenv("S3_BUCKET", raising=False)
    assert pipeline.bucket_name() == "merouane-nyc-taxi-raw-euw3-2026"
    monkeypatch.setenv("S3_BUCKET", "other")
    assert pipeline.bucket_name() == "other"


@responses.activate
def test_uploads_month(s3):
    mock_source(parquet_bytes())
    assert pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01") == "uploaded"
    assert list_keys(s3) == [pipeline.month_key("yellow", "2024-01")]


@responses.activate
def test_second_run_skips(s3):
    mock_source(parquet_bytes())
    pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01")
    before = n_gets()
    assert pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01") == "skipped"
    assert n_gets() == before


@responses.activate
def test_size_mismatch_reuploads(s3):
    s3.put_object(Bucket=BUCKET, Key=pipeline.month_key("yellow", "2024-01"), Body=b"x")
    mock_source(parquet_bytes())
    assert pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01") == "uploaded"


@responses.activate
def test_bad_schema_not_uploaded(s3):
    mock_source(parquet_bytes(["a", "b"]))
    with pytest.raises(pipeline.SchemaError, match="VendorID"):
        pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01")
    assert list_keys(s3) == []


@responses.activate
def test_http_retries_then_succeeds(s3):
    body = parquet_bytes()
    url = trip_url("yellow", "2024-01")
    responses.add(responses.HEAD, url, headers={"Content-Length": str(len(body))})
    responses.add(responses.GET, url, status=503)
    responses.add(responses.GET, url, status=503)
    responses.add(responses.GET, url, body=body)
    assert pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01") == "uploaded"
    assert n_gets() == 3


@responses.activate
def test_http_404_not_retried(s3):
    url = trip_url("yellow", "2024-01")
    responses.add(responses.HEAD, url, status=404)
    responses.add(responses.GET, url, status=404)
    with pytest.raises(requests.HTTPError):
        pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01")
    assert n_gets() == 1


@responses.activate
def test_http_gives_up_after_attempts(s3):
    url = trip_url("yellow", "2024-01")
    responses.add(responses.HEAD, url, status=503)
    responses.add(responses.GET, url, status=503)
    with pytest.raises(requests.HTTPError):
        pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01")
    assert n_gets() == pipeline.MAX_ATTEMPTS


@responses.activate
def test_s3_upload_retries(s3, monkeypatch):
    mock_source(parquet_bytes())
    real, calls = s3.upload_file, []

    def flaky(*a, **k):
        calls.append(1)
        if len(calls) < 3:
            raise ClientError(
                {"Error": {"Code": "SlowDown", "Message": "x"}}, "PutObject"
            )
        return real(*a, **k)

    monkeypatch.setattr(s3, "upload_file", flaky)
    assert pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01") == "uploaded"
    assert len(calls) == 3


@responses.activate
def test_s3_head_non_404_error_retried(s3, monkeypatch):
    mock_source(parquet_bytes())
    real, calls = s3.head_object, []

    def flaky(**k):
        calls.append(1)
        if len(calls) == 1:
            raise ClientError({"Error": {"Code": "500", "Message": "x"}}, "HeadObject")
        return real(**k)

    monkeypatch.setattr(s3, "head_object", flaky)
    assert pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01") == "uploaded"
    assert len(calls) == 2


@responses.activate
def test_zones_upload_and_skip(s3):
    csv = ZONES_CSV
    responses.add(responses.HEAD, ZONES_URL, headers={"Content-Length": str(len(csv))})
    responses.add(responses.GET, ZONES_URL, body=csv)
    assert pipeline.ingest_zones(s3, BUCKET) == "uploaded"
    assert pipeline.ingest_zones(s3, BUCKET) == "skipped"
    assert list_keys(s3) == [pipeline.ZONES_KEY]


@responses.activate
def test_cli(s3, monkeypatch, capsys):
    mock_source(parquet_bytes())
    monkeypatch.setattr(pipeline, "make_s3", lambda: s3)
    monkeypatch.setenv("S3_BUCKET", BUCKET)
    assert pipeline.main(["--months", "2024-01", "--service", "yellow"]) == 0
    assert "uploaded" in capsys.readouterr().out


def test_cli_rejects_bad_month():
    with pytest.raises(SystemExit):
        pipeline.main(["--months", "2024-13", "--service", "yellow"])


@responses.activate
def test_redirect_not_followed(s3):
    url = trip_url("yellow", "2024-01")
    responses.add(responses.HEAD, url, headers={"Content-Length": "1"})
    responses.add(
        responses.GET, url, status=302, headers={"Location": "http://evil.example/x"}
    )
    with pytest.raises(pipeline.DownloadError, match="redirect"):
        pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01")
    assert n_gets() == 1
    assert list_keys(s3) == []


@responses.activate
def test_requests_disable_redirects(s3, monkeypatch):
    seen = []
    for name in ("get", "head"):
        real = getattr(requests, name)

        def spy(*a, _real=real, **k):
            seen.append(k.get("allow_redirects"))
            return _real(*a, **k)

        monkeypatch.setattr(requests, name, spy)
    mock_source(parquet_bytes())
    pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01")
    assert seen and all(v is False for v in seen)


def test_max_bytes_env(monkeypatch):
    monkeypatch.delenv("MAX_DOWNLOAD_BYTES", raising=False)
    assert pipeline.max_download_bytes() == 2 * 1024**3
    monkeypatch.setenv("MAX_DOWNLOAD_BYTES", "123")
    assert pipeline.max_download_bytes() == 123


@responses.activate
def test_content_length_over_cap_refused(s3, monkeypatch):
    monkeypatch.setenv("MAX_DOWNLOAD_BYTES", "10")
    url = trip_url("yellow", "2024-01")
    responses.add(responses.HEAD, url, headers={"Content-Length": "5"})
    responses.add(responses.GET, url, body=b"x" * 50, headers={"Content-Length": "50"})
    with pytest.raises(pipeline.DownloadError, match="exceeds"):
        pipeline.ingest_month(s3, BUCKET, "yellow", "2024-01")
    assert list_keys(s3) == []


class FakeResp:
    """Server that lies about Content-Length (urllib3 would otherwise reject the mismatch)."""

    status_code, is_redirect, url = (
        200,
        False,
        "https://d37ci6vzurychx.cloudfront.net/x",
    )

    def __init__(self, declared, chunks):
        self.headers = {"Content-Length": str(declared)}
        self.chunks = chunks

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield from self.chunks

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _fake_get(monkeypatch, resp):
    monkeypatch.setattr(requests, "get", lambda *a, **k: resp)


def test_stream_over_cap_aborts_and_deletes(tmp_path, monkeypatch):
    monkeypatch.setenv("MAX_DOWNLOAD_BYTES", "10")
    _fake_get(monkeypatch, FakeResp(8, [b"x" * 6, b"x" * 6]))
    dest = tmp_path / "f.bin"
    with pytest.raises(pipeline.DownloadError, match="cap"):
        pipeline.stream_to_file("https://d37ci6vzurychx.cloudfront.net/x", dest)
    assert not dest.exists()


def test_stream_over_declared_length_aborts(tmp_path, monkeypatch):
    _fake_get(monkeypatch, FakeResp(5, [b"x" * 3, b"x" * 3]))
    dest = tmp_path / "f.bin"
    with pytest.raises(pipeline.DownloadError, match="declared"):
        pipeline.stream_to_file("https://d37ci6vzurychx.cloudfront.net/x", dest)
    assert not dest.exists()


@responses.activate
def test_zones_bad_header_not_uploaded(s3):
    csv = b'"LocationID","Borough"\n1,"EWR"\n'
    responses.add(responses.HEAD, ZONES_URL, headers={"Content-Length": str(len(csv))})
    responses.add(responses.GET, ZONES_URL, body=csv)
    with pytest.raises(pipeline.SchemaError, match="Zone"):
        pipeline.ingest_zones(s3, BUCKET)
    assert list_keys(s3) == []
