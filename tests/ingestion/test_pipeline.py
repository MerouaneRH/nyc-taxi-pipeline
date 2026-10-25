import io
from datetime import date

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


# --- review fixes: H2 unpublished, M2 mid-stream retry, L6 CLI/length ---


def _cli_env(s3, monkeypatch):
    monkeypatch.setattr(pipeline, "make_s3", lambda: s3)
    monkeypatch.setenv("S3_BUCKET", BUCKET)


@responses.activate
@pytest.mark.parametrize("status", [403, 404])
def test_skip_unpublished_exits_99(s3, monkeypatch, capsys, status):
    _cli_env(s3, monkeypatch)
    url = trip_url("yellow", "2030-01")
    responses.add(responses.HEAD, url, status=status)
    responses.add(responses.GET, url, status=status)
    code = pipeline.main(["--months", "2030-01", "--skip-unpublished"])
    assert code == pipeline.EXIT_UNPUBLISHED == 99
    assert f"unpublished {url}" in capsys.readouterr().err
    assert n_gets() == 1  # never retried
    assert list_keys(s3) == []


@responses.activate
def test_skip_unpublished_still_processes_other_months(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    bad = trip_url("yellow", "2030-01")
    responses.add(responses.HEAD, bad, status=403)
    responses.add(responses.GET, bad, status=403)
    mock_source(parquet_bytes(), "2024-01")
    code = pipeline.main(["--months", "2030-01", "2024-01", "--skip-unpublished"])
    assert code == 0  # something uploaded: dbt has work (catch-up window)
    assert list_keys(s3) == [pipeline.month_key("yellow", "2024-01")]


@responses.activate
@pytest.mark.parametrize("status", [403, 404])
def test_unpublished_without_flag_raises(s3, monkeypatch, status):
    _cli_env(s3, monkeypatch)
    url = trip_url("yellow", "2030-01")
    responses.add(responses.HEAD, url, status=status)
    responses.add(responses.GET, url, status=status)
    with pytest.raises(requests.HTTPError):
        pipeline.main(["--months", "2030-01"])
    assert n_gets() == 1


@responses.activate
def test_skip_unpublished_does_not_swallow_500(s3, monkeypatch):
    _cli_env(s3, monkeypatch)
    url = trip_url("yellow", "2030-01")
    responses.add(responses.HEAD, url, status=500)
    responses.add(responses.GET, url, status=500)
    with pytest.raises(requests.HTTPError):
        pipeline.main(["--months", "2030-01", "--skip-unpublished"])


@pytest.mark.parametrize(
    "exc",
    [
        requests.exceptions.ChunkedEncodingError("reset"),
        requests.exceptions.ContentDecodingError("bad"),
    ],
)
def test_midstream_reset_retries_then_succeeds(tmp_path, monkeypatch, exc):
    attempts = []

    class Flaky(FakeResp):
        def iter_content(self, chunk_size):
            attempts.append(1)
            if len(attempts) < 3:
                yield b"xx"
                raise exc
            yield from self.chunks

    monkeypatch.setattr(requests, "get", lambda *a, **k: Flaky(4, [b"xx", b"xx"]))
    dest = tmp_path / "f.bin"
    pipeline.stream_to_file("https://d37ci6vzurychx.cloudfront.net/x", dest)
    assert len(attempts) == 3
    assert dest.read_bytes() == b"xxxx"


def test_no_zones_no_months_is_usage_error(capsys):
    with pytest.raises(SystemExit) as e:
        pipeline.main([])
    assert e.value.code != 0
    assert "--zones" in capsys.readouterr().err


def test_stream_under_length_raises_and_deletes(tmp_path, monkeypatch):
    _fake_get(monkeypatch, FakeResp(10, [b"x" * 3, b"x" * 3]))
    dest = tmp_path / "f.bin"
    with pytest.raises(pipeline.DownloadError, match="short|incomplete|declared"):
        pipeline.stream_to_file("https://d37ci6vzurychx.cloudfront.net/x", dest)
    assert not dest.exists()


# --- catch-up window: several months, NEW_MONTHS line, exit codes ---


def _unpublished(month, status=403):
    url = trip_url("yellow", month)
    responses.add(responses.HEAD, url, status=status)
    responses.add(responses.GET, url, status=status)
    return url


def _preload(s3, month):
    body = parquet_bytes()
    s3.put_object(Bucket=BUCKET, Key=pipeline.month_key("yellow", month), Body=body)
    mock_source(body, month)


def _run_window(months, capsys):
    code = pipeline.main(["--months", *months, "--service", "yellow", "--skip-unpublished"])
    return code, capsys.readouterr()


@responses.activate
def test_window_uploaded_only(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    mock_source(parquet_bytes(), "2024-02")
    mock_source(parquet_bytes(), "2024-01")
    code, out = _run_window(["2024-02", "2024-01"], capsys)
    assert code == 0
    assert out.out.splitlines()[-1] == "NEW_MONTHS=2024-01 2024-02"


@responses.activate
def test_window_current_only_exits_99(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    _preload(s3, "2024-02")
    _preload(s3, "2024-01")
    code, out = _run_window(["2024-02", "2024-01"], capsys)
    assert code == 99
    assert out.out.splitlines()[-1] == "NEW_MONTHS="
    assert n_gets() == 0


@responses.activate
def test_window_unpublished_only_exits_99(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    url = _unpublished("2030-02")
    _unpublished("2030-01", 404)
    code, out = _run_window(["2030-02", "2030-01"], capsys)
    assert code == 99
    assert out.out.splitlines()[-1] == "NEW_MONTHS="
    assert f"unpublished {url}" in out.err


@responses.activate
def test_window_uploaded_and_unpublished_exits_0(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    _unpublished("2030-01")
    mock_source(parquet_bytes(), "2024-01")
    code, out = _run_window(["2030-01", "2024-01"], capsys)
    assert code == 0
    assert out.out.splitlines()[-1] == "NEW_MONTHS=2024-01"


@responses.activate
def test_window_current_and_unpublished_exits_99(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    _unpublished("2030-01")
    _preload(s3, "2024-01")
    code, out = _run_window(["2030-01", "2024-01"], capsys)
    assert code == 99
    assert out.out.splitlines()[-1] == "NEW_MONTHS="


@responses.activate
def test_window_new_months_oldest_first_and_only_uploaded(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    mock_source(parquet_bytes(), "2024-03")
    _unpublished("2030-01")
    _preload(s3, "2024-02")
    mock_source(parquet_bytes(), "2023-12")
    code, out = _run_window(["2024-03", "2030-01", "2024-02", "2023-12"], capsys)
    assert code == 0
    assert out.out.splitlines()[-1] == "NEW_MONTHS=2023-12 2024-03"
    assert "unpublished" not in out.out


@responses.activate
def test_window_other_errors_still_raise(s3, monkeypatch):
    _cli_env(s3, monkeypatch)
    _unpublished("2030-01", 500)
    with pytest.raises(requests.HTTPError):
        pipeline.main(["--months", "2030-01", "--skip-unpublished"])


@responses.activate
def test_without_flag_unchanged_but_prints_new_months(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    _preload(s3, "2024-01")
    code = pipeline.main(["--months", "2024-01"])
    out = capsys.readouterr().out
    assert code == 0  # nothing uploaded, no flag: still 0 as before
    assert out.splitlines()[-1] == "NEW_MONTHS="


@responses.activate
def test_zones_only_prints_no_new_months(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    responses.add(responses.HEAD, ZONES_URL, headers={"Content-Length": str(len(ZONES_CSV))})
    responses.add(responses.GET, ZONES_URL, body=ZONES_CSV)
    assert pipeline.main(["--zones"]) == 0
    assert "NEW_MONTHS" not in capsys.readouterr().out


# --- discovery: --since, --dry-run ---

TODAY = date(2026, 10, 25)


def test_expand_since_in_order_ends_at_current_month():
    months = pipeline.expand_since("2025-11", date(2026, 2, 3))
    assert months == ["2025-11", "2025-12", "2026-01", "2026-02"]


def test_expand_since_single_month_and_future():
    assert pipeline.expand_since("2026-10", TODAY) == ["2026-10"]
    assert pipeline.expand_since("2026-11", TODAY) == []


def test_expand_since_full_range_count():
    months = pipeline.expand_since("2024-01", TODAY)
    assert len(months) == 34 and months[0] == "2024-01" and months[-1] == "2026-10"


def test_since_and_months_mutually_exclusive(capsys):
    with pytest.raises(SystemExit) as exc:
        pipeline.main(["--since", "2024-01", "--months", "2024-02"])
    assert exc.value.code == 2
    assert "not allowed with" in capsys.readouterr().err


def test_since_bad_format_and_future_rejected(capsys):
    with pytest.raises(SystemExit):
        pipeline.main(["--since", "24-01"])
    with pytest.raises(SystemExit):
        pipeline.main(["--since", "2030-01"], today=TODAY)
    assert "after the current month" in capsys.readouterr().err


@responses.activate
def test_since_ingests_range_and_unpublished_middle_does_not_stop(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    mock_source(parquet_bytes(), "2024-01")
    _unpublished("2024-02")
    mock_source(parquet_bytes(), "2024-03")
    code = pipeline.main(
        ["--since", "2024-01", "--skip-unpublished"], today=date(2024, 3, 10)
    )
    out = capsys.readouterr()
    assert code == 0
    assert out.out.splitlines()[-1] == "NEW_MONTHS=2024-01 2024-03"
    assert "unpublished" in out.err
    assert len(list_keys(s3)) == 2


@responses.activate
def test_since_current_month_unpublished_exits_99(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    _preload(s3, "2024-01")
    _unpublished("2024-02")
    code = pipeline.main(
        ["--since", "2024-01", "--skip-unpublished"], today=date(2024, 2, 10)
    )
    assert code == 99
    assert capsys.readouterr().out.splitlines()[-1] == "NEW_MONTHS="


def _dry(capsys, months=("2024-01", "2024-02", "2024-03"), today=date(2024, 3, 10)):
    code = pipeline.main(
        ["--since", months[0], "--skip-unpublished", "--dry-run"], today=today
    )
    return code, capsys.readouterr().out.splitlines()


@responses.activate
def test_dry_run_reports_and_never_writes(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    _preload(s3, "2024-01")
    mock_source(parquet_bytes(), "2024-02")
    _unpublished("2024-03")
    calls = []
    monkeypatch.setattr(s3, "put_object", lambda **kw: calls.append("put"))
    monkeypatch.setattr(s3, "upload_file", lambda *a, **kw: calls.append("upload"))
    before = list_keys(s3)
    code, lines = _dry(capsys)
    assert lines == [
        "current 2024-01",
        "would-upload 2024-02",
        "unpublished 2024-03",
        "NEW_MONTHS=2024-02",
    ]
    assert code == 0
    assert calls == [] and list_keys(s3) == before
    assert n_gets() == 0


@responses.activate
def test_dry_run_size_mismatch_would_upload(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    s3.put_object(Bucket=BUCKET, Key=pipeline.month_key("yellow", "2024-01"), Body=b"x")
    mock_source(parquet_bytes(), "2024-01")
    code, lines = _dry(capsys, today=date(2024, 1, 10))
    assert lines == ["would-upload 2024-01", "NEW_MONTHS=2024-01"] and code == 0
    assert n_gets() == 0


@responses.activate
def test_dry_run_nothing_new_exits_99(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    _preload(s3, "2024-01")
    _unpublished("2024-02")
    code, lines = _dry(capsys, today=date(2024, 2, 10))
    assert lines == ["current 2024-01", "unpublished 2024-02", "NEW_MONTHS="]
    assert code == 99


@responses.activate
def test_dry_run_other_errors_still_raise(s3, monkeypatch):
    _cli_env(s3, monkeypatch)
    _unpublished("2024-01", 500)
    with pytest.raises(requests.HTTPError):
        pipeline.main(["--months", "2024-01", "--skip-unpublished", "--dry-run"])


# --- --until: inclusive upper bound for batch loads ---


def test_expand_since_until_bounds_window():
    assert pipeline.expand_since("2024-11", TODAY, until="2025-02") == [
        "2024-11", "2024-12", "2025-01", "2025-02",
    ]
    assert pipeline.expand_since("2024-05", TODAY, until="2024-05") == ["2024-05"]


def test_expand_since_until_past_current_month_is_capped():
    months = pipeline.expand_since("2026-09", TODAY, until="2027-03")
    assert months == ["2026-09", "2026-10"]


def test_until_requires_since(capsys):
    with pytest.raises(SystemExit) as exc:
        pipeline.main(["--months", "2024-01", "--until", "2024-03"])
    assert exc.value.code == 2
    assert "--until requires --since" in capsys.readouterr().err


def test_until_with_months_still_rejected(capsys):
    with pytest.raises(SystemExit):
        pipeline.main(["--since", "2024-01", "--months", "2024-02", "--until", "2024-03"])
    assert "not allowed with" in capsys.readouterr().err


def test_until_before_since_and_bad_format_rejected(capsys):
    with pytest.raises(SystemExit):
        pipeline.main(["--since", "2024-05", "--until", "2024-04"], today=TODAY)
    assert "before --since" in capsys.readouterr().err
    with pytest.raises(SystemExit):
        pipeline.main(["--since", "2024-05", "--until", "2024-13"], today=TODAY)
    with pytest.raises(SystemExit):
        pipeline.main(["--since", "2024-05", "--until", "may"], today=TODAY)


@responses.activate
def test_until_bounds_ingest_to_window(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    for month in ("2024-01", "2024-02"):
        mock_source(parquet_bytes(), month)  # 2024-03 is not mocked: touching it would fail
    code = pipeline.main(
        ["--since", "2024-01", "--until", "2024-02", "--skip-unpublished"],
        today=date(2024, 3, 10),
    )
    assert code == 0
    assert capsys.readouterr().out.splitlines()[-1] == "NEW_MONTHS=2024-01 2024-02"
    assert len(list_keys(s3)) == 2


@responses.activate
def test_until_dry_run_lists_only_window(s3, monkeypatch, capsys):
    _cli_env(s3, monkeypatch)
    _preload(s3, "2024-01")
    mock_source(parquet_bytes(), "2024-02")
    code = pipeline.main(
        ["--since", "2024-01", "--until", "2024-02", "--skip-unpublished", "--dry-run"],
        today=date(2024, 3, 10),
    )
    out = capsys.readouterr().out.splitlines()
    assert code == 0
    assert out == ["current 2024-01", "would-upload 2024-02", "NEW_MONTHS=2024-02"]
