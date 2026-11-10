"""Bronze files laid out as landing writes them, through DuckDB into silver."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from spotgrid_lakehouse.transforms.smard import (
    REBUILD_TABLE,
    SILVER_TABLE,
    differences,
    fetched_at,
    parse,
    rebuild,
    run,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "smard"
WEEK = 1790546400000  # 2026-09-27 22:00 UTC
HOUR = 3_600_000
NOW = datetime(2026, 10, 7, 22, tzinfo=UTC)
LOAD_QH = "smard/410/DE/quarterhour"


def silver(con) -> list[tuple]:
    return con.execute(
        f"SELECT series_id, region, resolution, epoch_ms(ts_utc), value, revision_count "
        f"FROM {SILVER_TABLE} ORDER BY ALL"
    ).fetchall()


def quarantined(con) -> list[tuple]:
    return con.execute(
        "SELECT coalesce(series_id, 0), reason, ts_utc IS NULL FROM silver.quarantine ORDER BY ALL"
    ).fetchall()


def revisions(con) -> list[tuple]:
    return con.execute(
        "SELECT old_value, new_value, day(detected_at) FROM silver.revisions ORDER BY detected_at"
    ).fetchall()


def test_landing_time_comes_from_the_path():
    key = "smard/4169/DE-LU/hour/fetch_date=2026-10-07/4169_DE-LU_hour_1_053012.json"
    assert fetched_at(key) == datetime(2026, 10, 7, 5, 30, 12, tzinfo=UTC)


def test_keys_come_from_the_landing_path():
    key = "smard/4169/DE-LU/hour/fetch_date=2026-10-07/4169_DE-LU_hour_1790546400000_210000.json"
    good, bad = parse(key, json.dumps({"series": [[WEEK, 150.71], [WEEK + HOUR, None]]}).encode())

    assert bad == []
    assert good == [(key, 4169, "DE-LU", "hour", WEEK, 150.71, NOW - timedelta(hours=1))]


def test_recorded_chunk_lands_whole(bronze, warehouse):
    recorded = FIXTURES / "4169" / "DE-LU" / "4169_DE-LU_quarterhour_1790546400000.json"
    series = json.loads(recorded.read_text())["series"]
    bronze.land(series, path="smard/4169/DE-LU/quarterhour")

    run(warehouse, bronze, now=NOW)

    rows = silver(warehouse)
    filled = [(ts, v) for ts, v in series if v is not None]
    assert len(rows) == len(filled) == 672
    assert [(r[3], r[4]) for r in rows] == filled


def test_newest_landing_of_a_slot_wins_and_counts_a_revision(bronze, warehouse):
    bronze.land([[WEEK, 10.0]], fetch_date="2026-10-06")
    bronze.land([[WEEK, 12.5]], fetch_date="2026-10-07")

    run(warehouse, bronze, now=NOW)

    assert silver(warehouse) == [(4169, "DE-LU", "hour", WEEK, 12.5, 0)]


def test_lookback_skips_older_landings(bronze, warehouse):
    bronze.land([[WEEK, 10.0]], fetch_date="2026-10-01")
    bronze.land([[WEEK + HOUR, 11.0]], fetch_date="2026-10-07")

    run(warehouse, bronze, lookback=timedelta(days=3), now=NOW)

    assert [r[3] for r in silver(warehouse)] == [WEEK + HOUR]


def test_later_runs_log_revisions_and_reruns_add_nothing(bronze, warehouse):
    bronze.land([[WEEK, 10.0]], fetch_date="2026-10-06")
    run(warehouse, bronze, now=NOW)
    bronze.land([[WEEK, 11.5]], fetch_date="2026-10-07")

    first = run(warehouse, bronze, now=NOW)
    again = run(warehouse, bronze, now=NOW)

    assert silver(warehouse) == [(4169, "DE-LU", "hour", WEEK, 11.5, 1)]
    assert revisions(warehouse) == [(10.0, 11.5, 7)]
    assert (first["updated"], first["revisions_logged"]) == (1, 1)
    assert (again["inserted"], again["updated"], again["revisions_logged"]) == (0, 0, 0)


def test_older_landing_never_overwrites(bronze, warehouse):
    bronze.land([[WEEK, 10.0]], fetch_date="2026-10-07")
    run(warehouse, bronze, now=NOW)
    bronze.land([[WEEK, 9.0]], fetch_date="2026-10-05")

    run(warehouse, bronze, now=NOW)

    assert silver(warehouse)[0][4] == 10.0
    assert revisions(warehouse) == []


def test_values_beyond_physical_limits_are_quarantined(bronze, warehouse):
    bronze.land([[WEEK, 88.5], [WEEK + HOUR, 5000.0]])
    # 30 GWh is a normal hour of load but impossible in a quarter-hour.
    bronze.land([[WEEK, 12000.0], [WEEK + 900_000, 30000.0]], path=LOAD_QH)
    # Austria's load is far below Germany's floor; the limits are Germany's only.
    bronze.land([[WEEK, 7000.0]], path="smard/410/AT/hour")

    run(warehouse, bronze, now=NOW)

    assert len(silver(warehouse)) == 3
    assert quarantined(warehouse) == [(410, "out_of_range", False), (4169, "out_of_range", False)]


def test_bad_timestamps_are_quarantined(bronze, warehouse):
    after_landing = WEEK + 15 * 24 * HOUR  # 5 days past the landing
    bronze.land([[WEEK + HOUR // 2, 80.0], [after_landing, 80.0]])

    run(warehouse, bronze, now=NOW)

    assert [r[1] for r in quarantined(warehouse)] == ["future_slot", "misaligned_slot"]


def test_unknown_series_and_paths_are_quarantined(bronze, warehouse):
    bronze.land([[WEEK, 1.0]], path="smard/9999/DE/hour")
    bronze.write("smard/elsewhere/fetch_date=2026-10-07/4169.json", b"{}")

    run(warehouse, bronze, now=NOW)

    assert silver(warehouse) == []
    assert quarantined(warehouse) == [(0, "unknown_path", True), (9999, "unknown_series", False)]


def test_changed_payload_shape_is_quarantined_without_failing(bronze, warehouse):
    bronze.land([], raw=b'{"data": []}')
    bronze.land([[WEEK, 12000.0], [WEEK + 900_000], [WEEK + 1_800_000, 1.0, 2.0]], path=LOAD_QH)

    run(warehouse, bronze, now=NOW)
    run(warehouse, bronze, now=NOW)

    assert len(silver(warehouse)) == 1
    assert quarantined(warehouse) == [
        (410, "bad_point", False),
        (410, "bad_point", False),
        (4169, "bad_payload", True),
    ]


def test_local_dates_follow_berlin_time(bronze, warehouse):
    # Summer: the local day starts at 22:00 UTC; winter: 23:00 UTC.
    summer = int(datetime(2026, 7, 1, 21, tzinfo=UTC).timestamp() * 1000)
    winter = int(datetime(2026, 1, 14, 22, tzinfo=UTC).timestamp() * 1000)
    bronze.land([[summer, 1.0], [summer + HOUR, 1.0], [winter, 1.0], [winter + HOUR, 1.0]],
                fetch_date="2026-07-02")  # fmt: skip

    run(warehouse, bronze, lookback=None, now=NOW)

    dates = warehouse.execute(f"SELECT local_date FROM {SILVER_TABLE} ORDER BY ts_utc").fetchall()
    assert [d[0] for d in dates] == [
        date(2026, 1, 14),
        date(2026, 1, 15),
        date(2026, 7, 1),
        date(2026, 7, 2),
    ]


def test_daylight_saving_days(bronze, warehouse):
    spring = int(datetime(2026, 3, 28, 22, tzinfo=UTC).timestamp() * 1000)
    autumn = int(datetime(2025, 10, 25, 21, tzinfo=UTC).timestamp() * 1000)
    points = [[spring + h * HOUR, 1.0] for h in range(26)]
    points += [[autumn + h * HOUR, 1.0] for h in range(28)]
    bronze.land(points, fetch_date="2026-03-30")

    run(warehouse, bronze, lookback=None, now=NOW)

    hours = dict(
        warehouse.execute(f"SELECT local_date, count(*) FROM {SILVER_TABLE} GROUP BY 1").fetchall()
    )
    assert (hours[date(2026, 3, 29)], hours[date(2025, 10, 26)]) == (23, 25)


def test_rebuild_matches_successive_merges(bronze, warehouse):
    later = WEEK + HOUR
    # 10 -> 12 -> 12 -> 10 is two revisions; the other slot changes once.
    for day, values in [(6, (10.0, 5.0)), (7, (12.0, 6.0)), (8, (12.0, 6.0)), (9, (10.0, 6.0))]:
        bronze.land([[WEEK, values[0]], [later, values[1]]], fetch_date=f"2026-10-0{day}")
        run(
            warehouse,
            bronze,
            lookback=timedelta(days=1),
            now=datetime(2026, 10, day, 23, tzinfo=UTC),
        )

    result = rebuild(warehouse, bronze, REBUILD_TABLE)

    assert result["rows"] == 2
    assert differences(warehouse, SILVER_TABLE, REBUILD_TABLE) == {"values": 0, "rows": 0}
    counts = warehouse.execute(
        f"SELECT revision_count FROM {REBUILD_TABLE} ORDER BY ts_utc"
    ).fetchall()
    assert counts == [(2,), (1,)]


def test_rebuild_with_revisions_logs_bronze_history(bronze, warehouse):
    for day, value in [(6, 10.0), (7, 12.0), (8, 12.0), (9, 10.0)]:
        bronze.land([[WEEK, value]], fetch_date=f"2026-10-0{day}")

    rebuild(warehouse, bronze, SILVER_TABLE, with_revisions=True)
    rebuild(warehouse, bronze, SILVER_TABLE, with_revisions=True)

    assert revisions(warehouse) == [(10.0, 12.0, 7), (12.0, 10.0, 9)]
    assert silver(warehouse) == [(4169, "DE-LU", "hour", WEEK, 10.0, 2)]
