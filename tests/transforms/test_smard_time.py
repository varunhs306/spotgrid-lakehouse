from datetime import UTC, date, datetime, timedelta

from pyspark.sql import functions as F

from spotgrid_lakehouse.transforms.smard import with_time_columns


def ms(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp() * 1000)


def times(spark, *utc: str) -> list[tuple[str, date]]:
    rows = spark.createDataFrame([(ms(t), 1.0) for t in utc], "ts_ms LONG, value DOUBLE")
    out = with_time_columns(rows).select(
        F.date_format("ts_utc", "yyyy-MM-dd'T'HH:mm").alias("ts_utc"), "local_date"
    )
    return [(r.ts_utc, r.local_date) for r in out.collect()]


def test_epoch_ms_becomes_utc_timestamp(spark):
    rows = spark.createDataFrame([(1790546400000, 1.0)], "ts_ms LONG, value DOUBLE")

    out = with_time_columns(rows)

    assert out.columns == ["value", "ts_utc", "local_date"]
    assert times(spark, "2026-09-27T22:00") == [("2026-09-27T22:00", date(2026, 9, 28))]


def test_local_day_starts_at_22_utc_in_summer(spark):
    assert times(spark, "2026-07-01T21:00", "2026-07-01T22:00") == [
        ("2026-07-01T21:00", date(2026, 7, 1)),
        ("2026-07-01T22:00", date(2026, 7, 2)),
    ]


def test_local_day_starts_at_23_utc_in_winter(spark):
    assert times(spark, "2026-01-14T22:00", "2026-01-14T23:00") == [
        ("2026-01-14T22:00", date(2026, 1, 14)),
        ("2026-01-14T23:00", date(2026, 1, 15)),
    ]


def hours_on(spark, day: date, first_utc: str, hours: int) -> int:
    start = datetime.fromisoformat(first_utc)
    slots = [(start + timedelta(hours=h)).isoformat(timespec="minutes") for h in range(hours)]
    return sum(local == day for _, local in times(spark, *slots))


def test_spring_forward_day_has_23_hours(spark):
    # 2026-03-29 runs from 2026-03-28T23:00 to 2026-03-29T22:00 UTC.
    assert hours_on(spark, date(2026, 3, 29), "2026-03-28T22:00", 26) == 23


def test_fall_back_day_has_25_hours(spark):
    # 2025-10-26 runs from 2025-10-25T22:00 to 2025-10-26T23:00 UTC.
    assert hours_on(spark, date(2025, 10, 26), "2025-10-25T21:00", 28) == 25
