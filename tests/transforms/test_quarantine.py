"""Bad rows injected into bronze are quarantined with a reason; the rest reach silver."""

from datetime import datetime

from spotgrid_lakehouse.transforms.smard import CHUNK_SCHEMA, split

ROOT = "/Volumes/workspace/bronze/landing"
FETCHED_AT = datetime(2026, 10, 7, 21, 0)
WEEK = 1790546400000  # 2026-09-27 22:00 UTC; landed 2026-10-07
HOUR = 3_600_000
PRICE = "smard/4169/DE-LU/hour/fetch_date=2026-10-07/4169_DE-LU_hour_1790546400000.json"
LOAD_QH = "smard/410/DE/quarterhour/fetch_date=2026-10-07/410_DE_quarterhour_1790546400000.json"


def chunks(spark, *files):
    """files: (path below the landing root, series as served, or None for an unreadable one)"""
    rows = [
        (
            f"{ROOT}/{path}",
            FETCHED_AT,
            None if series is None else [[float(x) for x in p] for p in series],
        )
        for path, series in files
    ]
    return spark.createDataFrame(rows, f"file_path STRING, fetched_at TIMESTAMP, {CHUNK_SCHEMA}")


def reasons(bad) -> list[tuple]:
    return sorted((r.series_id or 0, r.reason) for r in bad.collect())


def test_clean_chunk_has_nothing_to_quarantine(spark):
    good, bad = split(chunks(spark, (PRICE, [[WEEK, 88.5], [WEEK + HOUR, -1.2]])))

    assert good.count() == 2
    assert bad.count() == 0


def test_values_beyond_physical_limits_are_quarantined(spark):
    good, bad = split(
        chunks(
            spark,
            (PRICE, [[WEEK, 88.5], [WEEK + HOUR, 5000.0]]),
            # 30 GWh is a normal hour of load but impossible in a quarter-hour.
            (LOAD_QH, [[WEEK, 12000.0], [WEEK + 900_000, 30000.0]]),
        )
    )

    assert good.count() == 2
    assert reasons(bad) == [(410, "out_of_range"), (4169, "out_of_range")]


def test_bad_timestamps_are_quarantined(spark):
    half_past = WEEK + HOUR // 2
    after_landing = WEEK + 15 * 24 * HOUR  # 5 days past the landing
    _, bad = split(chunks(spark, (PRICE, [[half_past, 80.0], [after_landing, 80.0]])))

    assert sorted(r.reason for r in bad.collect()) == ["future_slot", "misaligned_slot"]


def test_unknown_series_and_paths_are_quarantined(spark):
    good, bad = split(
        chunks(
            spark,
            ("smard/9999/DE/hour/fetch_date=2026-10-07/9999_DE_hour_1.json", [[WEEK, 1.0]]),
            ("elsewhere/4169.json", [[WEEK, 1.0]]),
        )
    )

    assert good.count() == 0
    assert reasons(bad) == [(0, "unknown_path"), (9999, "unknown_series")]


def test_changed_payload_shape_is_quarantined_without_failing(spark):
    good, bad = split(
        chunks(
            spark,
            (PRICE, None),
            (LOAD_QH, [[WEEK, 12000.0], [WEEK + 900_000], [WEEK + 1_800_000, 11000.0, 1.0]]),
        )
    )

    assert good.count() == 1
    rows = sorted((r.series_id, r.reason, r.ts_utc is None) for r in bad.collect())
    assert rows == [
        (410, "bad_point", False),
        (410, "bad_point", False),
        (4169, "bad_payload", True),
    ]
