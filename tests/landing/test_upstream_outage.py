from datetime import UTC, datetime

import httpx
import pytest

from spotgrid_lakehouse.landing.smard import land
from spotgrid_lakehouse.sources.smard import Resolution, Series, SmardClient

WEEK = 1790546400000
NOW = datetime(2026, 10, 7, 5, tzinfo=UTC)


class Upstream:
    """SMARD stand-in that answers 503 while it is down."""

    def __init__(self):
        self.down = True
        self.calls = 0

    def __call__(self, request):
        self.calls += 1
        if self.down:
            return httpx.Response(503)
        return httpx.Response(200, content=b'{"series": [[1790546400000, 80.0]]}')


def run(upstream, volume):
    http = httpx.Client(transport=httpx.MockTransport(upstream))
    client = SmardClient(
        http, base_url="https://smard.test", min_interval_s=0, sleep=lambda s: None
    )
    with client:
        return land(client, volume, Series.PRICE_DE_LU, Resolution.HOUR, [WEEK], now=lambda: NOW)


def test_outage_is_retried_then_fails_without_landing(volume):
    upstream = Upstream()

    with pytest.raises(httpx.HTTPStatusError):
        run(upstream, volume)

    assert upstream.calls == 5
    assert volume.writes == []


def test_next_run_after_an_outage_lands_the_chunk(volume):
    upstream = Upstream()
    with pytest.raises(httpx.HTTPStatusError):
        run(upstream, volume)
    upstream.down = False

    assert len(run(upstream, volume)) == 1
