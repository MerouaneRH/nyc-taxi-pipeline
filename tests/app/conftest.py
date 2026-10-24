import pytest

from app import freshness


@pytest.fixture(autouse=True)
def _no_aws_freshness(monkeypatch):
    """Never query Athena for the data version in tests."""
    monkeypatch.setattr(freshness, "query_version", lambda: "test-version")
    freshness._store().update(v=None)
    yield
