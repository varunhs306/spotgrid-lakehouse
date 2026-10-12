import json

from spotgrid_lakehouse.monitoring.__main__ import main


def test_dry_run_writes_the_summary_from_step_reports(tmp_path):
    landing = tmp_path / "landing.json"
    landing.write_text(
        json.dumps(
            {
                "landed": [
                    {
                        "series": "LOAD",
                        "series_id": 410,
                        "resolution": "hour",
                        "checked": 2,
                        "written": 1,
                        "dates": ["2026-10-05", "2026-10-18"],
                    }
                ]
            }
        )
    )
    summary = tmp_path / "summary.md"

    main(
        [
            "--started-at", "2026-10-12T15:17:00+00:00",
            "--landing", str(landing),
            "--manifest", str(tmp_path / "missing.json"),
            "--outcome", "land bronze=success",
            "--outcome", "merge silver=failure",
            "--dry-run",
        ],
        env={"GITHUB_STEP_SUMMARY": str(summary)},
    )  # fmt: skip

    text = summary.read_text(encoding="utf-8")
    assert "**Failed at: merge silver**" in text
    assert "| LOAD hour | 2 | 1 | 2026-10-05 → 2026-10-18 |" in text
    assert "### Export" not in text


def test_tomorrow_and_quiet_mornings():
    from datetime import UTC, date, datetime

    from spotgrid_lakehouse.monitoring.__main__ import quiet, tomorrow

    afternoon = datetime(2026, 10, 12, 15, 17, tzinfo=UTC)
    morning = datetime(2026, 10, 12, 5, 17, tzinfo=UTC)

    assert tomorrow(afternoon) == date(2026, 10, 13)
    assert tomorrow(morning) is None
    assert quiet("schedule", morning)
    assert not quiet("schedule", afternoon)
    assert not quiet("workflow_dispatch", morning)
