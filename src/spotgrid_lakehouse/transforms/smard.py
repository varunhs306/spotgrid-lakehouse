"""SMARD chunks from the bronze volume to silver time-series rows."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# The JSON body of one chunk file; meta_data is not needed downstream.
CHUNK_SCHEMA = "series ARRAY<ARRAY<DOUBLE>>"

# smard/{series}/{region}/{resolution}/fetch_date=YYYY-MM-DD/{chunk}.json, as written by landing.
PATH_PATTERN = r"/smard/(\d+)/([^/]+)/([a-z]+)/fetch_date=[^/]+/[^/]+\.json$"


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
