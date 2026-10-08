import json
from datetime import datetime
from pathlib import Path

from spotgrid_lakehouse.transforms.smard import CHUNK_SCHEMA, parse_chunks

FIXTURES = Path(__file__).parents[1] / "fixtures" / "smard"
ROOT = "/Volumes/workspace/bronze/landing"
FETCHED_AT = datetime(2026, 10, 7, 21, 0)


def chunk_frame(spark, rows):
    """rows: (path below the landing root, [[epoch_ms, value], ...])"""
    as_doubles = [
        [[float(x) if x is not None else None for x in point] for point in series]
        for _, series in rows
    ]
    return spark.createDataFrame(
        [(f"{ROOT}/{path}", FETCHED_AT, s) for (path, _), s in zip(rows, as_doubles, strict=True)],
        f"file_path STRING, fetched_at TIMESTAMP, {CHUNK_SCHEMA}",
    )


def test_keys_come_from_the_landing_path(spark):
    path = "smard/4169/DE-LU/hour/fetch_date=2026-10-07/4169_DE-LU_hour_1790546400000.json"
    rows = parse_chunks(chunk_frame(spark, [(path, [[1790546400000, 150.71]])])).collect()

    assert [r.asDict() for r in rows] == [
        {
            "series_id": 4169,
            "region": "DE-LU",
            "resolution": "hour",
            "ts_ms": 1790546400000,
            "value": 150.71,
            "fetched_at": FETCHED_AT,
        }
    ]


def test_future_slots_are_dropped(spark):
    path = "smard/410/DE/hour/fetch_date=2026-10-07/410_DE_hour_1791151200000.json"
    series = [[1791151200000, 41000.5], [1791154800000, None], [1791158400000, None]]

    rows = parse_chunks(chunk_frame(spark, [(path, series)])).collect()

    assert [(r.ts_ms, r.value) for r in rows] == [(1791151200000, 41000.5)]


def test_recorded_chunk_keeps_every_filled_slot(spark):
    recorded = FIXTURES / "4169" / "DE-LU" / "4169_DE-LU_quarterhour_1790546400000.json"
    series = json.loads(recorded.read_text())["series"]
    path = f"smard/4169/DE-LU/quarterhour/fetch_date=2026-10-07/{recorded.name}"

    rows = parse_chunks(chunk_frame(spark, [(path, series)])).collect()

    filled = [(ts, v) for ts, v in series if v is not None]
    assert len(rows) == len(filled) == 672
    assert {r.resolution for r in rows} == {"quarterhour"}
    assert [(r.ts_ms, r.value) for r in rows] == filled
