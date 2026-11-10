"""A small warehouse with every catalog source for two days, built through silver and dbt."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from spotgrid_lakehouse.gold import dbt
from spotgrid_lakehouse.landing.volume import LocalVolume
from spotgrid_lakehouse.sources.catalog import Kind, sources
from spotgrid_lakehouse.transforms.smard import connect, run

# Two German local days in October 2026, the second a fall-back day with 25 hours.
START = datetime(2026, 10, 23, 22, tzinfo=UTC)
HOURS = 49
SLOT_MS = {"hour": 3_600_000, "quarterhour": 900_000}
LANDED = datetime(2026, 10, 26, 6, tzinfo=UTC)


def value(series, region: str, slot: int, resolution: str) -> float:
    """Deterministic, plausible values: prices around 80, volumes in the low thousands."""
    if series.kind == Kind.PRICE:
        return round(80.0 + (series.id % 7) + (slot % 24) * 0.5 - (12 if slot % 24 == 3 else 0), 2)
    base = 1000.0 + series.id % 13 * 10 + len(region)
    if series.id in (4068, 125):  # dark from 18:00 to 08:00 Berlin time
        local_hour = (START.hour + slot + 2) % 24
        base = 0.0 if local_hour < 8 or local_hour >= 18 else 2000.0
    if series.column == "load_mwh":
        base = 30000.0 if region == "DE" else 5000.0
    return base / (4 if resolution == "quarterhour" else 1)


def land_everything(bronze: LocalVolume) -> None:
    start_ms = int(START.timestamp() * 1000)
    for series, region in sources():
        if series.id == 251:  # DE-AT-LU ended in 2018
            continue
        for resolution, step in SLOT_MS.items():
            slots = HOURS * 3_600_000 // step
            points = [
                [start_ms + i * step, value(series, region, i * step // 3_600_000, resolution)]
                for i in range(slots)
            ]
            key = (
                f"smard/{series.id}/{region}/{resolution}/fetch_date={LANDED:%Y-%m-%d}/"
                f"{series.id}_{region}_{resolution}_{start_ms}_{LANDED:%H%M%S}.json"
            )
            bronze.write(key, json.dumps({"series": points}).encode())


@pytest.fixture(scope="session")
def built(tmp_path_factory):
    root = tmp_path_factory.mktemp("lake")
    bronze = LocalVolume(root / "bronze")
    land_everything(bronze)
    path = str(root / "warehouse.duckdb")
    con = connect(path)
    run(con, bronze, lookback=None, now=LANDED)
    con.close()
    assert dbt(["build", "--quiet"], warehouse=path)
    return SimpleNamespace(path=path, bronze=bronze, start=START, landed=LANDED)
