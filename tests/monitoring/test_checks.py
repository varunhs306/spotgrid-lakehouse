from datetime import UTC, date, datetime, timedelta

import pytest

from spotgrid_lakehouse.monitoring.checks import (
    completeness,
    horizon,
    hours_in_day,
    missing_hours,
    price_revisions,
)
from spotgrid_lakehouse.sources.smard import Series

# 17:17 in Berlin (CEST): tomorrow's prices are due.
AFTERNOON = datetime(2026, 10, 12, 15, 17, tzinfo=UTC)
MORNING = datetime(2026, 10, 12, 6, 0, tzinfo=UTC)
SERIES = (4169, 410, 4067, 1225, 4068, 123, 3791, 125)


def complete(now, window_days=14):
    """Full coverage of every series up to its horizon."""
    rows = []
    for series_id in SERIES:
        last = horizon(Series(series_id), now)
        day = now.date() - timedelta(days=window_days)
        while day <= last:
            rows.append((series_id, day, hours_in_day(day)))
            day += timedelta(days=1)
    return rows


@pytest.mark.parametrize(
    ("day", "hours"),
    [(date(2026, 3, 29), 23), (date(2026, 10, 25), 25), (date(2026, 10, 12), 24)],
)
def test_local_days_follow_daylight_saving(day, hours):
    assert hours_in_day(day) == hours


def test_horizons():
    assert horizon(Series.PRICE_DE_LU, AFTERNOON) == date(2026, 10, 13)
    assert horizon(Series.PRICE_DE_LU, MORNING) == date(2026, 10, 12)
    assert horizon(Series.LOAD, AFTERNOON) == date(2026, 10, 11)
    assert horizon(Series.FORECAST_SOLAR, AFTERNOON) == date(2026, 10, 12)


def test_complete_data_has_no_findings():
    assert missing_hours(complete(AFTERNOON), AFTERNOON) == []


def test_a_day_without_prices_is_reported():
    rows = [r for r in complete(AFTERNOON) if r[:2] != (4169, date(2026, 10, 10))]

    findings = missing_hours(rows, AFTERNOON)

    assert len(findings) == 1
    assert findings[0].key == "missing:PRICE_DE_LU"
    assert findings[0].days == {"2026-10-10": "0/24 h"}


def test_partial_day_and_late_prices_for_tomorrow():
    rows = [
        (s, d, 20 if (s, d) == (410, date(2026, 10, 11)) else h)
        for s, d, h in complete(AFTERNOON)
        if (s, d) != (4169, date(2026, 10, 13))
    ]

    findings = {f.key: f.days for f in missing_hours(rows, AFTERNOON)}

    assert findings == {
        "missing:PRICE_DE_LU": {"2026-10-13": "0/24 h"},
        "missing:LOAD": {"2026-10-11": "20/24 h"},
    }


def test_tomorrow_is_not_due_in_the_morning():
    assert missing_hours(complete(MORNING), MORNING) == []


def test_only_price_revisions_are_flagged():
    revised = [
        (4169, date(2026, 10, 9), 3),
        (410, date(2026, 10, 9), 40),
        (4169, date(2026, 10, 8), 0),
    ]

    findings = price_revisions(revised)

    assert [(f.key, f.days) for f in findings] == [
        ("revised:PRICE_DE_LU", {"2026-10-09": "3 h changed"})
    ]
    assert price_revisions([(410, date(2026, 10, 9), 40)]) == []


def test_winter_run_still_expects_tomorrows_prices():
    # 15:17 UTC is 16:17 in Berlin under CET.
    winter = datetime(2026, 12, 7, 15, 17, tzinfo=UTC)

    assert horizon(Series.PRICE_DE_LU, winter) == date(2026, 12, 8)


def test_completeness_by_group():
    rows = [
        (s, d, 16 if (s, d) == (410, date(2026, 10, 11)) else h)
        for s, d, h in complete(AFTERNOON)
        if (s, d) != (4169, date(2026, 10, 10))
    ]

    groups = completeness(rows, missing_hours(rows, AFTERNOON), AFTERNOON)

    assert groups["Prices"] == {
        "to": "2026-10-09",
        "due": "2026-10-13",
        "problems": {"2026-10-10": "0/24 h"},
    }
    assert groups["Actuals"] == {
        "to": "2026-10-10",
        "due": "2026-10-11",
        "problems": {"2026-10-11": "16/24 h LOAD"},
    }
    assert groups["Forecasts"]["problems"] == {}
