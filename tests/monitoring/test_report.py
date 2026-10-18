from datetime import UTC, date, datetime, timedelta

import httpx

from spotgrid_lakehouse.monitoring.checks import horizon, hours_in_day
from spotgrid_lakehouse.monitoring.report import build, check_api, dbt_summary, markdown
from spotgrid_lakehouse.sources.smard import Series

STARTED = datetime(2026, 10, 12, 15, 17, tzinfo=UTC)
NOW = STARTED + timedelta(minutes=4, seconds=2)
RUN = {"url": "https://github.com/o/r/actions/runs/1", "trigger": "schedule"}
OK = dict.fromkeys(("land bronze", "merge silver", "build gold", "export"), "success")
LANDING = {
    "landed": [
        {
            "series": "PRICE_DE_LU",
            "series_id": 4169,
            "resolution": "hour",
            "checked": 2,
            "written": 1,
            "dates": ["2026-10-05", "2026-10-18"],
        }
    ]
}
MANIFEST = {
    "exported_at": "2026-10-12T15:21:00+00:00",
    "tables": {"hourly_features": {"rows": 18192, "max_time": "2026-10-13T21:00:00+00:00"}},
}
DBT = {
    "results": [
        {"unique_id": "model.spotgrid.hourly_features", "status": "success"},
        {"unique_id": "test.spotgrid.unique_hourly_features_ts_utc", "status": "pass"},
        {"unique_id": "test.spotgrid.not_null_daily_price_summary_local_date", "status": "fail"},
        {"unique_id": "model.spotgrid.daily_price_summary", "status": "skipped"},
    ]
}


def coverage(skip=()):
    rows = []
    for series in Series:
        if series == Series.PRICE_DE_AT_LU:
            continue
        day = NOW.date() - timedelta(days=14)
        while day <= horizon(series, NOW):
            if (series.value, day) not in skip:
                rows.append(
                    {"series_id": series.value, "local_date": day, "hours": hours_in_day(day)}
                )
            day += timedelta(days=1)
    return rows


def report(**overrides):
    args = {
        "run": RUN,
        "started_at": STARTED,
        "now": NOW,
        "outcomes": OK,
        "seconds": {"land bronze": 28, "merge silver": 129},
        "landing": LANDING,
        "silver_changes": [
            {"series_id": 4169, "local_date": date(2026, 10, 13), "inserted": 24, "revised": 0},
            {"series_id": 4068, "local_date": date(2026, 10, 11), "inserted": 0, "revised": 36},
            {"series_id": 4068, "local_date": date(2026, 10, 10), "inserted": 0, "revised": 4},
        ],
        "coverage": coverage(),
        "dbt": dbt_summary({"results": DBT["results"][:2]}),
        "manifest": MANIFEST,
        "api": {"ok": True, "status": 200, "ms": 120, "age_minutes": 3},
    }
    return build(**(args | overrides))


def test_clean_run():
    result = report()

    assert result["run"]["status"] == "success"
    assert result["run"]["seconds"] == 242
    assert result["silver"] == [
        {"series": "SOLAR", "inserted": 0, "revised": 40},
        {"series": "PRICE_DE_LU", "inserted": 24, "revised": 0},
    ]
    assert result["findings"] == []


def test_first_failed_step_is_named():
    outcomes = OK | {"merge silver": "failure", "build gold": "skipped", "export": "skipped"}

    result = report(outcomes=outcomes, silver_changes=None, dbt=None, manifest=None)

    assert result["run"]["status"] == "failure"
    assert result["run"]["failed_step"] == "merge silver"


def test_api_down_fails_the_run():
    result = report(api={"ok": False, "status": 502})

    assert result["run"]["failed_step"] == "API check"


def test_gaps_and_price_changes_become_findings():
    changes = [{"series_id": 4169, "local_date": date(2026, 10, 9), "inserted": 0, "revised": 2}]

    result = report(coverage=coverage(skip={(4169, date(2026, 10, 10))}), silver_changes=changes)

    assert [(f["key"], f["days"]) for f in result["findings"]] == [
        ("missing:PRICE_DE_LU", {"2026-10-10": "0/24 h"}),
        ("revised:PRICE_DE_LU", {"2026-10-09": "2 h changed"}),
    ]


def test_dbt_counts():
    assert dbt_summary(DBT) == {
        "passed": 2,
        "failed": 1,
        "warned": 0,
        "skipped": 1,
        "failed_nodes": ["not_null_daily_price_summary_local_date"],
    }
    assert dbt_summary(None) is None


def test_markdown_summary():
    result = report(coverage=coverage(skip={(4169, date(2026, 10, 10))}))

    text = markdown(result, {"missing:PRICE_DE_LU": "https://github.com/o/r/issues/7"})

    assert text.startswith("## ✅ daily · 2026-10-12 15:17 UTC · 4m02s\n")
    assert "| land bronze | ✅ | 28s |" in text
    assert "| merge silver | ✅ | 2m09s |" in text
    assert "| PRICE_DE_LU hour | 2 | 1 | 2026-10-05 → 2026-10-18 |" in text
    assert "| SOLAR | 0 | 40 |" in text
    assert "| hourly_features | 18,192 | 2026-10-13T21:00:00+00:00 |" in text
    assert "✅ 120 ms, data exported 3 min ago" in text
    assert (
        "- ⚠️ [Missing hours: PRICE_DE_LU](https://github.com/o/r/issues/7): 2026-10-10 0/24 h"
        in text
    )


def test_quarantine_summary_by_reason():
    quarantine = [{"reason": "out_of_range", "rows": 3}, {"reason": "bad_point", "rows": 1}]

    result = report(quarantine=quarantine)
    text = markdown(result)

    assert result["quality"] == {
        "quarantined": 4,
        "by_reason": {"bad_point": 1, "out_of_range": 3},
    }
    assert "| out_of_range | 3 |" in text
    assert "Nothing quarantined." in markdown(report(quarantine=[]))


def test_markdown_of_a_failed_run_skips_missing_sections():
    skipped = dict.fromkeys(("merge silver", "build gold", "export"), "skipped")
    outcomes = OK | {"land bronze": "failure"} | skipped
    result = report(
        outcomes=outcomes, landing=None, silver_changes=None, coverage=None, dbt=None, manifest=None
    )

    text = markdown(result)

    assert text.startswith("## ❌ daily")
    assert "**Failed at: land bronze**" in text
    assert "### Bronze" not in text
    assert "| export | ⏭️ | - |" in text


def test_check_api():
    def handler(request):
        assert request.url.path == "/v1/freshness"
        return httpx.Response(200, json={"age_minutes": 5})

    http = httpx.Client(transport=httpx.MockTransport(handler))

    result = check_api("https://api.example/", http=http)

    assert result["ok"] is True
    assert result["age_minutes"] == 5
    assert result["tomorrow"] is None
    assert check_api(None) is None


def test_check_api_reads_tomorrows_prices():
    def handler(request):
        if request.url.path == "/v1/prices/daily":
            assert request.url.params["start"] == "2026-10-13"
            return httpx.Response(200, json={"rows": [{"avg_price_eur_mwh": 197.2}]})
        return httpx.Response(200, json={"age_minutes": 0})

    http = httpx.Client(transport=httpx.MockTransport(handler))

    result = check_api("https://api.example", date(2026, 10, 13), http)

    assert result["tomorrow"] == {"avg_price_eur_mwh": 197.2}


def test_check_api_unreachable():
    def handler(request):
        raise httpx.ConnectError("down")

    http = httpx.Client(transport=httpx.MockTransport(handler))

    assert check_api("https://api.example", http=http) == {"ok": False, "error": "ConnectError"}
