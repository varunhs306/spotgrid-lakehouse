import httpx
import pytest

from spotgrid_lakehouse.sources.smard import Resolution, Series, SmardClient, default_region

BASE = "https://smard.test/app/chart_data"


class FakeTime:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def client_for(handler, fake_time=None, **kwargs) -> SmardClient:
    fake_time = fake_time or FakeTime()
    return SmardClient(
        httpx.Client(transport=httpx.MockTransport(handler)),
        base_url=BASE,
        sleep=fake_time.sleep,
        clock=fake_time.clock,
        **kwargs,
    )


def replies(*responses):
    """Handler that returns (or raises) the given responses in order, counting calls."""
    queue = list(responses)

    def handler(request):
        handler.calls += 1
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    handler.calls = 0
    return handler


def test_default_region_uses_bidding_zone_for_prices():
    assert default_region(Series.PRICE_DE_LU) == "DE-LU"
    assert default_region(Series.PRICE_DE_AT_LU) == "DE-AT-LU"
    assert default_region(Series.LOAD) == "DE"


def test_index_builds_url_and_sorts_timestamps():
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(200, json={"timestamps": [2000, 1000]})

    assert client_for(handler).index(Series.LOAD, Resolution.QUARTERHOUR) == [1000, 2000]
    assert seen == [f"{BASE}/410/DE/index_quarterhour.json"]


def test_chunk_returns_raw_bytes():
    body = b'{"meta_data": {"version": 1}, "series": [[1000, 1.5], [2000, null]]}'
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(200, content=body)

    assert client_for(handler).chunk(Series.PRICE_DE_LU, Resolution.HOUR, 1000) == body
    assert seen == [f"{BASE}/4169/DE-LU/4169_DE-LU_hour_1000.json"]


def test_missing_chunk_raises_without_retry():
    handler = replies(httpx.Response(404))
    with pytest.raises(httpx.HTTPStatusError):
        client_for(handler).chunk(Series.SOLAR, Resolution.HOUR, 1000)
    assert handler.calls == 1


def test_retries_server_errors_with_exponential_backoff():
    handler = replies(httpx.Response(503), httpx.Response(502), httpx.Response(200, content=b"{}"))
    fake_time = FakeTime()
    assert client_for(handler, fake_time).chunk(Series.LOAD, Resolution.HOUR, 1000) == b"{}"
    assert fake_time.sleeps == [1.0, 2.0]


def test_retries_transport_errors():
    handler = replies(httpx.ConnectTimeout("slow"), httpx.Response(200, content=b"{}"))
    assert client_for(handler).chunk(Series.LOAD, Resolution.HOUR, 1000) == b"{}"
    assert handler.calls == 2


def test_gives_up_after_max_attempts():
    handler = replies(*[httpx.Response(500)] * 3)
    with pytest.raises(httpx.HTTPStatusError):
        client_for(handler, max_attempts=3).chunk(Series.LOAD, Resolution.HOUR, 1000)
    assert handler.calls == 3


def test_honours_retry_after_on_rate_limit():
    handler = replies(
        httpx.Response(429, headers={"Retry-After": "7"}), httpx.Response(200, content=b"{}")
    )
    fake_time = FakeTime()
    client_for(handler, fake_time).chunk(Series.LOAD, Resolution.HOUR, 1000)
    assert fake_time.sleeps == [7.0]


def test_backoff_is_capped():
    handler = replies(*[httpx.Response(500)] * 4, httpx.Response(200, content=b"{}"))
    fake_time = FakeTime()
    client_for(handler, fake_time, max_backoff_s=3.0).chunk(Series.LOAD, Resolution.HOUR, 1000)
    assert fake_time.sleeps == [1.0, 2.0, 3.0, 3.0]


def test_spaces_out_consecutive_requests():
    handler = replies(*[httpx.Response(200, content=b"{}")] * 2)
    fake_time = FakeTime()
    client = client_for(handler, fake_time, min_interval_s=0.5)
    client.chunk(Series.LOAD, Resolution.HOUR, 1000)
    client.chunk(Series.LOAD, Resolution.HOUR, 2000)
    assert fake_time.sleeps == [0.5]
