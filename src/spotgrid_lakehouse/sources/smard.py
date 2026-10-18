"""Client for the SMARD chart-data API. Data: Bundesnetzagentur | SMARD.de (CC BY 4.0)."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from itertools import pairwise
from types import TracebackType

import httpx

BASE_URL = "https://www.smard.de/app/chart_data"
USER_AGENT = "spotgrid-lakehouse (+https://github.com/varunhs306/spotgrid-lakehouse)"
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
SLOT_MS = {"hour": 3_600_000, "quarterhour": 900_000}


class Series(IntEnum):
    PRICE_DE_LU = 4169
    PRICE_DE_AT_LU = 251
    LOAD = 410
    WIND_ONSHORE = 4067
    WIND_OFFSHORE = 1225
    SOLAR = 4068
    FORECAST_WIND_ONSHORE = 123
    FORECAST_WIND_OFFSHORE = 3791
    FORECAST_SOLAR = 125


class Resolution(StrEnum):
    HOUR = "hour"
    QUARTERHOUR = "quarterhour"


# SMARD ignores the region for prices; the bidding zone is set by the series id.
_PRICE_REGIONS = {Series.PRICE_DE_LU: "DE-LU", Series.PRICE_DE_AT_LU: "DE-AT-LU"}


def default_region(series: Series) -> str:
    return _PRICE_REGIONS.get(series, "DE")


def index_path(series: Series, resolution: Resolution, region: str | None = None) -> str:
    region = region or default_region(series)
    return f"{series.value}/{region}/index_{resolution}.json"


def chunk_name(
    series: Series, resolution: Resolution, start_ms: int, region: str | None = None
) -> str:
    region = region or default_region(series)
    return f"{series.value}_{region}_{resolution}_{start_ms}.json"


def chunk_path(
    series: Series, resolution: Resolution, start_ms: int, region: str | None = None
) -> str:
    region = region or default_region(series)
    return f"{series.value}/{region}/{chunk_name(series, resolution, start_ms, region)}"


class SmardClient:
    def __init__(
        self,
        http: httpx.Client | None = None,
        base_url: str = BASE_URL,
        *,
        max_attempts: int = 5,
        backoff_s: float = 1.0,
        max_backoff_s: float = 30.0,
        min_interval_s: float = 0.5,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._http = http or httpx.Client(timeout=30, headers={"User-Agent": USER_AGENT})
        self._base_url = base_url.rstrip("/")
        self._max_attempts = max_attempts
        self._backoff_s = backoff_s
        self._max_backoff_s = max_backoff_s
        self._min_interval_s = min_interval_s
        self._sleep = sleep
        self._clock = clock
        self._last_request_at: float | None = None

    def index(self, series: Series, resolution: Resolution, region: str | None = None) -> list[int]:
        """Start of every weekly chunk, in epoch milliseconds, oldest first."""
        return sorted(self.get_raw(index_path(series, resolution, region)).json()["timestamps"])

    def chunk(
        self, series: Series, resolution: Resolution, start_ms: int, region: str | None = None
    ) -> bytes:
        """One weekly chunk exactly as served, so bronze can store it unchanged."""
        return self.get_raw(chunk_path(series, resolution, start_ms, region)).content

    def url(self, path: str) -> str:
        return f"{self._base_url}/{path}"

    def get_raw(self, path: str) -> httpx.Response:
        """GET a path below the base URL, with retries and rate limiting."""
        return self._get(self.url(path))

    def _get(self, url: str) -> httpx.Response:
        attempt = 1
        while True:
            self._wait_for_turn()
            try:
                response = self._http.get(url)
            except httpx.TransportError:
                if attempt >= self._max_attempts:
                    raise
                delay = self._backoff(attempt)
            else:
                if response.status_code not in RETRY_STATUSES or attempt >= self._max_attempts:
                    response.raise_for_status()
                    return response
                delay = self._retry_after(response)
                if delay is None:
                    delay = self._backoff(attempt)
            self._sleep(delay)
            attempt += 1

    def _wait_for_turn(self) -> None:
        if self._last_request_at is not None:
            wait = self._last_request_at + self._min_interval_s - self._clock()
            if wait > 0:
                self._sleep(wait)
        self._last_request_at = self._clock()

    def _backoff(self, attempt: int) -> float:
        return min(self._backoff_s * 2 ** (attempt - 1), self._max_backoff_s)

    def _retry_after(self, response: httpx.Response) -> float | None:
        try:
            return min(float(response.headers["Retry-After"]), self._max_backoff_s)
        except (KeyError, ValueError):  # absent, or the HTTP-date form
            return None

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> SmardClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


def shape_problems(
    index: object, chunk: object, resolution: Resolution, start_ms: int
) -> list[str]:
    """Where SMARD's JSON no longer has the shape the pipeline reads: an index of epoch-ms
    chunk starts, and a chunk of [epoch_ms, value or null] pairs, one per slot from `start_ms`.
    """
    problems = []
    starts = index.get("timestamps") if isinstance(index, dict) else None
    if not isinstance(starts, list) or not starts or not all(isinstance(t, int) for t in starts):
        problems.append("index: 'timestamps' is not a list of epoch milliseconds")
    series = chunk.get("series") if isinstance(chunk, dict) else None
    if not isinstance(series, list) or not series:
        return [*problems, "chunk: 'series' is missing or empty"]
    if not all(isinstance(p, list) and len(p) == 2 for p in series):
        return [*problems, "chunk: points are not [timestamp, value] pairs"]
    times = [p[0] for p in series]
    if not all(isinstance(t, int) for t in times):
        problems.append("chunk: timestamps are not epoch milliseconds")
    elif times[0] != start_ms or any(b - a != SLOT_MS[resolution] for a, b in pairwise(times)):
        problems.append(f"chunk: slots do not run every {resolution} from the chunk start")
    values = [p[1] for p in series]
    if not all(v is None or isinstance(v, int | float) for v in values):
        problems.append("chunk: values are not numbers or null")
    elif all(v is None for v in values):
        problems.append("chunk: every value is null")
    return problems


def check_shapes(client: SmardClient) -> list[str]:
    """Shape problems in each series' index and its last complete weekly chunk."""
    problems = []
    for series in Series:
        for resolution in Resolution:
            index = client.get_raw(index_path(series, resolution)).json()
            start = sorted(index["timestamps"])[-2]
            chunk = json.loads(client.chunk(series, resolution, start))
            problems += [
                f"{series.name} {resolution}: {problem}"
                for problem in shape_problems(index, chunk, resolution, start)
            ]
    return problems


def _utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Fetch one weekly SMARD chunk and summarise it.")
    parser.add_argument("--series", default=Series.PRICE_DE_LU.name, choices=Series.__members__)
    parser.add_argument("--resolution", default=Resolution.HOUR, type=Resolution)
    parser.add_argument("--weeks-ago", default=1, type=int, help="0 is the current, partial week")
    parser.add_argument(
        "--check", action="store_true", help="check every series' JSON shape and exit non-zero"
    )
    args = parser.parse_args(argv)

    if args.check:
        with SmardClient() as client:
            problems = check_shapes(client)
        print("\n".join(problems) or "SMARD JSON shape as expected for every series")
        sys.exit(1 if problems else 0)

    series = Series[args.series]
    with SmardClient() as client:
        start = client.index(series, args.resolution)[-1 - args.weeks_ago]
        points = json.loads(client.chunk(series, args.resolution, start))["series"]

    filled = [(ts, v) for ts, v in points if v is not None]
    print(f"{series.name} {args.resolution} chunk {_utc(start)}")
    print(f"slots={len(points)} filled={len(filled)}")
    if filled:
        values = [v for _, v in filled]
        print(f"min={min(values)} max={max(values)} last={_utc(filled[-1][0])}")


if __name__ == "__main__":
    main()
