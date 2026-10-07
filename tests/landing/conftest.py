from pathlib import Path

import httpx
import pytest

from spotgrid_lakehouse.sources.smard import SmardClient

FIXTURES = Path(__file__).parents[1] / "fixtures" / "smard"
BASE = "https://smard.test/app/chart_data"


class FakeVolume:
    def __init__(self):
        self.files: dict[str, bytes] = {}
        self.writes: list[str] = []

    def write(self, path: str, data: bytes) -> None:
        self.files[path] = data
        self.writes.append(path)


def serve_fixtures(request: httpx.Request) -> httpx.Response:
    path = FIXTURES / request.url.path.removeprefix("/app/chart_data/")
    return httpx.Response(200, content=path.read_bytes()) if path.exists() else httpx.Response(404)


@pytest.fixture
def volume():
    return FakeVolume()


@pytest.fixture
def client():
    http = httpx.Client(transport=httpx.MockTransport(serve_fixtures))
    with SmardClient(http, base_url=BASE, min_interval_s=0) as client:
        yield client
