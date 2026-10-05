"""Tests against real SMARD responses recorded by scripts/record_smard_fixtures.py."""

import json
from itertools import pairwise
from pathlib import Path

import httpx
import pytest

from spotgrid_lakehouse.sources.smard import Resolution, Series, SmardClient, chunk_path

FIXTURES = Path(__file__).parents[1] / "fixtures" / "smard"
BASE = "https://smard.test/app/chart_data"
HOUR_MS = 3_600_000
WEEK_2026_09_28 = 1790546400000
WEEK_2026_10_05 = 1791151200000
LAST_DE_AT_LU_WEEK = 1537740000000


def serve_fixtures(request: httpx.Request) -> httpx.Response:
    path = FIXTURES / request.url.path.removeprefix("/app/chart_data/")
    return httpx.Response(200, content=path.read_bytes()) if path.exists() else httpx.Response(404)


@pytest.fixture
def client():
    http = httpx.Client(transport=httpx.MockTransport(serve_fixtures))
    with SmardClient(http, base_url=BASE, min_interval_s=0) as client:
        yield client


def points(client, series, resolution, start):
    return json.loads(client.chunk(series, resolution, start))["series"]


def test_chunk_bytes_are_unchanged(client):
    raw = client.chunk(Series.PRICE_DE_LU, Resolution.HOUR, WEEK_2026_09_28)
    expected = FIXTURES / chunk_path(Series.PRICE_DE_LU, Resolution.HOUR, WEEK_2026_09_28)
    assert raw == expected.read_bytes()


def test_de_lu_price_index_starts_at_zone_change(client):
    index = client.index(Series.PRICE_DE_LU, Resolution.HOUR)
    assert index[0] == 1538344800000  # 2018-10-01 00:00 Europe/Berlin
    assert index == sorted(index)


def test_hourly_week_has_168_hourly_slots(client):
    week = points(client, Series.PRICE_DE_LU, Resolution.HOUR, WEEK_2026_09_28)
    assert len(week) == 168
    assert all(b[0] - a[0] == HOUR_MS for a, b in pairwise(week))
    assert all(value is not None for _, value in week)


def test_smard_hourly_price_is_mean_of_quarter_hours(client):
    hours = points(client, Series.PRICE_DE_LU, Resolution.HOUR, WEEK_2026_09_28)
    quarters = points(client, Series.PRICE_DE_LU, Resolution.QUARTERHOUR, WEEK_2026_09_28)
    assert len(quarters) == 4 * len(hours)
    for i, (_, hourly) in enumerate(hours):
        four = [value for _, value in quarters[4 * i : 4 * i + 4]]
        assert hourly == pytest.approx(sum(four) / 4, abs=0.006)


def test_future_slots_are_trailing_nulls(client):
    week = points(client, Series.LOAD, Resolution.HOUR, WEEK_2026_10_05)
    filled = [value is not None for _, value in week]
    assert 0 < sum(filled) < len(week)
    assert filled == sorted(filled, reverse=True)


def test_de_at_lu_series_ends_where_de_lu_begins(client):
    week = points(client, Series.PRICE_DE_AT_LU, Resolution.HOUR, LAST_DE_AT_LU_WEEK)
    last_filled = max(ts for ts, value in week if value is not None)
    assert last_filled + HOUR_MS == 1538344800000


def test_unrecorded_chunk_is_404(client):
    with pytest.raises(httpx.HTTPStatusError):
        client.chunk(Series.SOLAR, Resolution.HOUR, WEEK_2026_09_28)
