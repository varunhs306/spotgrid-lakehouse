"""Bronze files on disk, laid out as landing writes them, through to silver update rows."""

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from spotgrid_lakehouse.transforms.smard import (
    latest_per_slot,
    parse_chunks,
    read_chunks,
    with_time_columns,
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="Spark lists local files through Hadoop, which needs winutils"
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "smard"
WEEK = 1790546400000
SERIES_DIR = Path("smard/4169/DE-LU/hour")
CHUNK = f"4169_DE-LU_hour_{WEEK}.json"


def land(root: Path, fetch_date: str, series: list, landed_at: datetime) -> None:
    folder = root / SERIES_DIR / f"fetch_date={fetch_date}"
    folder.mkdir(parents=True, exist_ok=True)
    chunk = folder / CHUNK
    chunk.write_text(json.dumps({"meta_data": {"version": 1}, "series": series}))
    (folder / "_manifest.json").write_text(json.dumps({"files": {CHUNK: {}}}))
    (root / SERIES_DIR / "_index.json").write_text(json.dumps({"chunks": {}}))
    os.utime(chunk, (landed_at.timestamp(), landed_at.timestamp()))


def updates(spark, root: Path, since: datetime | None = None):
    rows = with_time_columns(parse_chunks(read_chunks(spark, root.as_posix(), since)))
    return latest_per_slot(rows).select(
        "series_id",
        "region",
        "resolution",
        F.unix_millis("ts_utc").alias("ts_ms"),
        "value",
        F.date_format("fetched_at", "yyyy-MM-dd").alias("landed"),
    )


def test_recorded_chunk_lands_in_silver_shape(spark, tmp_path):
    recorded = FIXTURES / "4169" / "DE-LU" / CHUNK
    series = json.loads(recorded.read_text())["series"]
    land(tmp_path, "2026-10-07", series, datetime(2026, 10, 7, 21, tzinfo=UTC))

    rows = updates(spark, tmp_path).orderBy("ts_ms").collect()

    assert len(rows) == 168
    assert {(r.series_id, r.region, r.resolution) for r in rows} == {(4169, "DE-LU", "hour")}
    assert [(r.ts_ms, r.value) for r in rows] == [tuple(p) for p in series]


def test_manifest_and_index_are_not_read_as_chunks(spark, tmp_path):
    land(tmp_path, "2026-10-07", [[WEEK, 1.0]], datetime(2026, 10, 7, 21, tzinfo=UTC))

    assert updates(spark, tmp_path).count() == 1


def test_newest_landing_of_a_slot_wins(spark, tmp_path):
    land(tmp_path, "2026-10-06", [[WEEK, 10.0]], datetime(2026, 10, 6, 21, tzinfo=UTC))
    land(tmp_path, "2026-10-07", [[WEEK, 12.5]], datetime(2026, 10, 7, 21, tzinfo=UTC))

    rows = updates(spark, tmp_path).collect()

    assert [(r.value, r.landed) for r in rows] == [(12.5, "2026-10-07")]


def test_lookback_skips_older_landings(spark, tmp_path):
    land(tmp_path, "2026-10-01", [[WEEK, 10.0]], datetime(2026, 10, 1, 21, tzinfo=UTC))
    land(tmp_path, "2026-10-07", [[WEEK + 3_600_000, 11.0]], datetime(2026, 10, 7, 21, tzinfo=UTC))

    rows = updates(spark, tmp_path, since=datetime(2026, 10, 5, tzinfo=UTC)).collect()

    assert [(r.ts_ms, r.landed) for r in rows] == [(WEEK + 3_600_000, "2026-10-07")]
