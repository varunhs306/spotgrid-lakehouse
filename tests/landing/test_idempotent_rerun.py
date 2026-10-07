from datetime import UTC, datetime

import httpx
import pytest

from spotgrid_lakehouse.landing.smard import land
from spotgrid_lakehouse.sources.smard import Resolution, Series, SmardClient

WEEK = 1790546400000
DAY_1 = datetime(2026, 10, 7, 5, tzinfo=UTC)
DAY_2 = datetime(2026, 10, 8, 5, tzinfo=UTC)


class Upstream:
    """SMARD stand-in whose chunk body the test can change between runs."""

    def __init__(self):
        self.body = b'{"series": [[1790546400000, 80.0]]}'

    def __call__(self, request):
        return httpx.Response(200, content=self.body)


class CrashingVolume:
    """Fails on the first write whose path ends with `fail_on`."""

    def __init__(self, inner, fail_on):
        self.inner, self.fail_on = inner, fail_on

    def read(self, path):
        return self.inner.read(path)

    def write(self, path, data):
        if self.fail_on and path.endswith(self.fail_on):
            self.fail_on = None
            raise OSError("upload interrupted")
        self.inner.write(path, data)


@pytest.fixture
def upstream():
    return Upstream()


@pytest.fixture
def client(upstream):
    http = httpx.Client(transport=httpx.MockTransport(upstream))
    with SmardClient(http, base_url="https://smard.test", min_interval_s=0) as client:
        yield client


def run(client, volume, now):
    return land(client, volume, Series.PRICE_DE_LU, Resolution.HOUR, [WEEK], now=lambda: now)


def test_rerun_uploads_nothing(client, volume):
    run(client, volume, DAY_1)
    before = list(volume.writes)

    assert run(client, volume, DAY_1) == []
    assert run(client, volume, DAY_2) == []
    assert volume.writes == before


def test_revised_chunk_lands_again_under_new_fetch_date(client, volume, upstream):
    run(client, volume, DAY_1)
    upstream.body = b'{"series": [[1790546400000, 81.5]]}'

    written = run(client, volume, DAY_2)

    assert written == [f"smard/4169/DE-LU/hour/fetch_date=2026-10-08/4169_DE-LU_hour_{WEEK}.json"]
    assert volume.files[written[0]] == upstream.body
    assert run(client, volume, DAY_2) == []


@pytest.mark.parametrize("fail_on", [".json", "_manifest.json", "_index.json"])
def test_interrupted_run_is_completed_by_the_next(client, volume, fail_on):
    with pytest.raises(OSError):
        run(client, CrashingVolume(volume, fail_on), DAY_1)

    assert len(run(client, volume, DAY_1)) == 1
    assert run(client, volume, DAY_1) == []
