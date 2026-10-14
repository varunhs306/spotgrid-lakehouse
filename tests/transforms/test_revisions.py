from datetime import UTC, date, datetime

from spotgrid_lakehouse.transforms.smard import revised, revision_history

SLOT = datetime(2026, 7, 5, 22, tzinfo=UTC)
LANDINGS = (
    "series_id INT, region STRING, resolution STRING, ts_utc TIMESTAMP, local_date DATE, "
    "value DOUBLE, fetched_at TIMESTAMP"
)
SILVER = (
    "series_id INT, region STRING, resolution STRING, ts_utc TIMESTAMP, value DOUBLE, "
    "last_changed_at TIMESTAMP"
)


def on(day: int) -> datetime:
    return datetime(2026, 10, day, tzinfo=UTC)


def landed(day: int, value: float) -> tuple:
    return (1225, "DE", "hour", SLOT, date(2026, 7, 6), value, on(day))


def silver(value: float, changed_day: int) -> tuple:
    return (1225, "DE", "hour", SLOT, value, on(changed_day))


def logged(revisions) -> list[tuple]:
    rows = revisions.orderBy("detected_at").collect()
    return [(r.old_value, r.new_value, r.detected_at.day) for r in rows]


def test_history_logs_one_row_per_change(spark):
    landings = [landed(6, 10.0), landed(7, 12.0), landed(8, 12.0), landed(9, 10.0)]
    rows = spark.createDataFrame(landings, LANDINGS)

    assert logged(revision_history(rows)) == [(10.0, 12.0, 7), (12.0, 10.0, 9)]


def test_first_landing_is_not_a_revision(spark):
    rows = spark.createDataFrame([landed(6, 10.0)], LANDINGS)

    assert revision_history(rows).count() == 0


def test_batch_logs_a_changed_value(spark):
    updates = spark.createDataFrame([landed(13, 11.5)], LANDINGS)
    current = spark.createDataFrame([silver(10.0, 6)], SILVER)

    assert logged(revised(updates, current)) == [(10.0, 11.5, 13)]


def test_batch_ignores_unchanged_and_older_landings(spark):
    updates = spark.createDataFrame([landed(13, 10.0), landed(5, 9.0)], LANDINGS)
    current = spark.createDataFrame([silver(10.0, 6)], SILVER)

    assert revised(updates, current).count() == 0
