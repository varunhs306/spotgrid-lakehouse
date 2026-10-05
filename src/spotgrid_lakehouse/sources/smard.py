"""Client for the SMARD chart-data API. Data: Bundesnetzagentur | SMARD.de (CC BY 4.0)."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from types import TracebackType

import httpx

BASE_URL = "https://www.smard.de/app/chart_data"
USER_AGENT = "spotgrid-lakehouse (+https://github.com/varunhs306/spotgrid-lakehouse)"


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


class SmardClient:
    def __init__(self, http: httpx.Client | None = None, base_url: str = BASE_URL) -> None:
        self._http = http or httpx.Client(timeout=30, headers={"User-Agent": USER_AGENT})
        self._base_url = base_url.rstrip("/")

    def index(self, series: Series, resolution: Resolution, region: str | None = None) -> list[int]:
        """Start of every weekly chunk, in epoch milliseconds, oldest first."""
        region = region or default_region(series)
        url = f"{self._base_url}/{series.value}/{region}/index_{resolution}.json"
        return sorted(self._get(url).json()["timestamps"])

    def chunk(
        self, series: Series, resolution: Resolution, start_ms: int, region: str | None = None
    ) -> bytes:
        """One weekly chunk exactly as served, so bronze can store it unchanged."""
        region = region or default_region(series)
        name = f"{series.value}_{region}_{resolution}_{start_ms}.json"
        return self._get(f"{self._base_url}/{series.value}/{region}/{name}").content

    def _get(self, url: str) -> httpx.Response:
        response = self._http.get(url)
        response.raise_for_status()
        return response

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
