from datetime import UTC, date, datetime

from spotgrid_lakehouse.transforms.smard import rebuild_rows

SLOT = datetime(2026, 10, 5, 22, tzinfo=UTC)
SCHEMA = (
    "series_id INT, region STRING, resolution STRING, ts_utc TIMESTAMP, local_date DATE, "
    "value DOUBLE, fetched_at TIMESTAMP"
)


def landed(day: int, value: float, slot: datetime = SLOT) -> tuple:
    return (
        4169,
        "DE-LU",
        "hour",
        slot,
        date(2026, 10, 6),
        value,
        datetime(2026, 10, day, tzinfo=UTC),
    )


def rebuilt(spark, *landings: tuple) -> list[tuple]:
    rows = rebuild_rows(spark.createDataFrame(list(landings), SCHEMA)).orderBy("ts_utc")
    return [
        (r.value, r.first_seen_at.day, r.last_changed_at.day, r.revision_count)
        for r in rows.collect()
    ]


def test_one_landing(spark):
    assert rebuilt(spark, landed(6, 10.0)) == [(10.0, 6, 6, 0)]


def test_unchanged_relandings_are_not_revisions(spark):
    assert rebuilt(spark, landed(6, 10.0), landed(7, 10.0), landed(8, 10.0)) == [(10.0, 6, 6, 0)]


def test_each_change_counts_like_a_merge(spark):
    # 10 -> 12 -> 12 -> 10: two revisions, the last on day 9.
    landings = [landed(6, 10.0), landed(7, 12.0), landed(8, 12.0), landed(9, 10.0)]
    assert rebuilt(spark, *reversed(landings)) == [(10.0, 6, 9, 2)]


def test_slots_are_independent(spark):
    later = datetime(2026, 10, 5, 23, tzinfo=UTC)
    assert rebuilt(spark, landed(6, 10.0), landed(7, 5.0, later), landed(8, 6.0, later)) == [
        (10.0, 6, 6, 0),
        (6.0, 7, 8, 1),
    ]
