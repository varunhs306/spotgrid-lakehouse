"""SMARD chunks from the bronze volume to silver time-series rows."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# The JSON body of one chunk file; meta_data is not needed downstream.
CHUNK_SCHEMA = "series ARRAY<ARRAY<DOUBLE>>"

# smard/{series}/{region}/{resolution}/fetch_date=YYYY-MM-DD/{chunk}.json, as written by landing.
PATH_PATTERN = r"/smard/(\d+)/([^/]+)/([a-z]+)/fetch_date=[^/]+/[^/]+\.json$"
# Market days and the daily summaries follow German local time.
LOCAL_TZ = "Europe/Berlin"


def parse_chunks(chunks: DataFrame) -> DataFrame:
    """One row per filled slot of each chunk.

    Expects `file_path`, `fetched_at` and `series` ([epoch_ms, value] pairs). SMARD serves
    future slots as null, so those are dropped.
    """
    path = F.col("file_path")
    return (
        chunks.select(
            F.regexp_extract(path, PATH_PATTERN, 1).cast("int").alias("series_id"),
            F.regexp_extract(path, PATH_PATTERN, 2).alias("region"),
            F.regexp_extract(path, PATH_PATTERN, 3).alias("resolution"),
            F.explode("series").alias("point"),
            "fetched_at",
        )
        .select(
            "series_id",
            "region",
            "resolution",
            F.col("point")[0].cast("long").alias("ts_ms"),
            F.col("point")[1].alias("value"),
            "fetched_at",
        )
        .where(F.col("value").isNotNull())
    )


def with_time_columns(rows: DataFrame) -> DataFrame:
    """Replace `ts_ms` (slot start, epoch ms) with `ts_utc` and the slot's `local_date`.

    The session time zone must be UTC: `to_date` reads the shifted timestamp in it.
    """
    ts_utc = F.timestamp_millis("ts_ms")
    return rows.withColumns(
        {"ts_utc": ts_utc, "local_date": F.to_date(F.from_utc_timestamp(ts_utc, LOCAL_TZ))}
    ).drop("ts_ms")
