import httpx
import pytest

from spotgrid_lakehouse.sources.smard import Resolution, Series, SmardClient, default_region

BASE = "https://smard.test/app/chart_data"


def client_for(handler) -> SmardClient:
    return SmardClient(httpx.Client(transport=httpx.MockTransport(handler)), base_url=BASE)


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


def test_missing_chunk_raises():
    client = client_for(lambda request: httpx.Response(404))
    with pytest.raises(httpx.HTTPStatusError):
        client.chunk(Series.SOLAR, Resolution.HOUR, 1000)
