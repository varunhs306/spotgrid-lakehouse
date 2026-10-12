"""Irregularities in the published data: missing hours and changed day-ahead prices.

Each finding has a stable key, so the same problem maps to the same GitHub issue run after run.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from spotgrid_lakehouse.sources.smard import Series

BERLIN = ZoneInfo("Europe/Berlin")
# Matches the landing re-fetch window: older gaps can no longer be filled by the daily run.
WINDOW_DAYS = 14
# Day-ahead results are out shortly after 13:00; SMARD shows them some time later.
PRICES_FOR_TOMORROW_FROM = time(14, 0)

# Actuals are complete for yesterday; forecasts and prices for today.
ACTUALS = (Series.LOAD, Series.WIND_ONSHORE, Series.WIND_OFFSHORE, Series.SOLAR)
FORECASTS = (Series.FORECAST_WIND_ONSHORE, Series.FORECAST_WIND_OFFSHORE, Series.FORECAST_SOLAR)


@dataclass(frozen=True)
class Finding:
    key: str
    title: str
    # Local day -> what is wrong with it, e.g. "0/24 h".
    days: dict[str, str] = field(default_factory=dict)


def hours_in_day(day: date) -> int:
    """23, 24 or 25: German local days change length with daylight saving time."""
    # Subtracting two times in the same zone ignores the offset change, so compare in UTC.
    start = datetime.combine(day, time(), BERLIN).astimezone(UTC)
    end = datetime.combine(day + timedelta(days=1), time(), BERLIN).astimezone(UTC)
    return int((end - start).total_seconds() // 3600)


def horizon(series: Series, now: datetime) -> date:
    """Last local day this series must have complete by `now`."""
    local = now.astimezone(BERLIN)
    today = local.date()
    if series in ACTUALS:
        return today - timedelta(days=1)
    if series == Series.PRICE_DE_LU and local.time() >= PRICES_FOR_TOMORROW_FROM:
        return today + timedelta(days=1)
    return today


def missing_hours(
    coverage: Iterable[tuple[int, date, int]], now: datetime, window_days: int = WINDOW_DAYS
) -> list[Finding]:
    """One finding per series with local days short of hours, from (series id, day, hours)."""
    hours = {(series_id, day): count for series_id, day, count in coverage}
    first = now.astimezone(BERLIN).date() - timedelta(days=window_days)
    findings = []
    for series in (Series.PRICE_DE_LU, *ACTUALS, *FORECASTS):
        last = horizon(series, now)
        short = {}
        day = first
        while day <= last:
            expected = hours_in_day(day)
            found = hours.get((series.value, day), 0)
            if found < expected:
                short[day.isoformat()] = f"{found}/{expected} h"
            day += timedelta(days=1)
        if short:
            findings.append(
                Finding(f"missing:{series.name}", f"Missing hours: {series.name}", short)
            )
    return findings


def price_revisions(revised: Iterable[tuple[int, date, int]]) -> list[Finding]:
    """Day-ahead prices are final once the auction is published, so any change is flagged."""
    days = {
        day.isoformat(): f"{count} h changed"
        for series_id, day, count in revised
        if series_id == Series.PRICE_DE_LU and count
    }
    if not days:
        return []
    return [Finding("revised:PRICE_DE_LU", "Published day-ahead prices changed", days)]
