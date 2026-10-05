"""Client for the SMARD chart-data API. Data: Bundesnetzagentur | SMARD.de (CC BY 4.0)."""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from types import TracebackType

import httpx

BASE_URL = "https://www.smard.de/app/chart_data"
USER_AGENT = "spotgrid-lakehouse (+https://github.com/varunhs306/spotgrid-lakehouse)"
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


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


def chunk_path(
    series: Series, resolution: Resolution, start_ms: int, region: str | None = None
) -> str:
    region = region or default_region(series)
    return f"{series.value}/{region}/{series.value}_{region}_{resolution}_{start_ms}.json"


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

    def get_raw(self, path: str) -> httpx.Response:
        """GET a path below the base URL, with retries and rate limiting."""
        return self._get(f"{self._base_url}/{path}")

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


def _utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Fetch one weekly SMARD chunk and summarise it.")
    parser.add_argument("--series", default=Series.PRICE_DE_LU.name, choices=Series.__members__)
    parser.add_argument("--resolution", default=Resolution.HOUR, type=Resolution)
    parser.add_argument("--weeks-ago", default=1, type=int, help="0 is the current, partial week")
    args = parser.parse_args(argv)

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
