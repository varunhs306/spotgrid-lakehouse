"""SMARD chunks from the bronze volume to silver time-series rows."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spotgrid_lakehouse.landing.volume import LANDING_ROOT
from spotgrid_lakehouse.sources.smard import Resolution, Series

SILVER_TABLE = "workspace.silver.smard_timeseries"
REBUILD_TABLE = "workspace.silver.smard_timeseries_rebuild"
REVISIONS_TABLE = "workspace.silver.revisions"
QUARANTINE_TABLE = "workspace.silver.quarantine"
# The JSON body of one chunk file; meta_data is not needed downstream.
CHUNK_SCHEMA = "series ARRAY<ARRAY<DOUBLE>>"
# Chunk files start with the series id; this skips _manifest.json and _index.json.
CHUNK_GLOB = "[0-9]*.json"
# smard/{series}/{region}/{resolution}/fetch_date=YYYY-MM-DD/{chunk}.json, as written by landing.
PATH_PATTERN = r"/smard/(\d+)/([^/]+)/([a-z]+)/fetch_date=[^/]+/[^/]+\.json$"
# Market days and the daily summaries follow German local time.
LOCAL_TZ = "Europe/Berlin"
KEY = ["series_id", "region", "resolution", "ts_utc"]

SILVER_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  series_id INT NOT NULL COMMENT 'SMARD series id, e.g. 4169 = day-ahead price DE-LU',
  region STRING NOT NULL,
  resolution STRING NOT NULL COMMENT 'hour or quarterhour',
  ts_utc TIMESTAMP NOT NULL COMMENT 'Slot start',
  local_date DATE NOT NULL COMMENT 'Slot start date in Europe/Berlin',
  value DOUBLE NOT NULL COMMENT 'EUR/MWh for prices, MWh per slot for volumes',
  first_seen_at TIMESTAMP NOT NULL COMMENT 'Landing time of the first chunk with this slot',
  last_changed_at TIMESTAMP NOT NULL COMMENT 'Landing time of the chunk that set the value',
  revision_count INT NOT NULL COMMENT 'Times the published value changed'
)
USING DELTA
COMMENT 'SMARD time series at native resolution. Bundesnetzagentur | SMARD.de (CC BY 4.0)'
"""

# A value only changes when a newer landing disagrees, so replaying old chunks is a no-op.
MERGE_SQL = """
MERGE INTO {table} AS t
USING {source} AS s
ON t.series_id = s.series_id AND t.region = s.region
  AND t.resolution = s.resolution AND t.ts_utc = s.ts_utc
WHEN MATCHED AND s.value <> t.value AND s.fetched_at > t.last_changed_at THEN UPDATE SET
  value = s.value, last_changed_at = s.fetched_at, revision_count = t.revision_count + 1
WHEN NOT MATCHED THEN INSERT
  (series_id, region, resolution, ts_utc, local_date, value,
   first_seen_at, last_changed_at, revision_count)
  VALUES (s.series_id, s.region, s.resolution, s.ts_utc, s.local_date, s.value,
   s.fetched_at, s.fetched_at, 0)
"""


REVISIONS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  series_id INT NOT NULL,
  region STRING NOT NULL,
  resolution STRING NOT NULL,
  ts_utc TIMESTAMP NOT NULL COMMENT 'Slot start',
  local_date DATE NOT NULL COMMENT 'Slot start date in Europe/Berlin',
  old_value DOUBLE NOT NULL,
  new_value DOUBLE NOT NULL,
  detected_at TIMESTAMP NOT NULL COMMENT 'Landing time of the chunk with the new value'
)
USING DELTA
COMMENT 'One row per change SMARD made to a published value'
"""

# Keyed on the landing that carried the change, so a rerun or a replay of bronze adds nothing.
REVISIONS_MERGE_SQL = """
MERGE INTO {table} AS t
USING {source} AS s
ON t.series_id = s.series_id AND t.region = s.region
  AND t.resolution = s.resolution AND t.ts_utc = s.ts_utc AND t.detected_at = s.detected_at
WHEN NOT MATCHED THEN INSERT *
"""
REVISION_COLUMNS = [*KEY, "local_date", "old_value", "new_value", "detected_at"]

QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  file_path STRING NOT NULL COMMENT 'Bronze chunk the row came from',
  series_id INT,
  region STRING,
  resolution STRING,
  ts_utc TIMESTAMP COMMENT 'Slot start; null when the chunk could not be read',
  value DOUBLE,
  reason STRING NOT NULL COMMENT 'Why the row was kept out of silver',
  fetched_at TIMESTAMP NOT NULL COMMENT 'Landing time of the chunk',
  quarantined_at TIMESTAMP NOT NULL COMMENT 'Run that first quarantined the row'
)
USING DELTA
COMMENT 'Rows from bronze kept out of silver, with a reason code'
"""

# A rerun, or the lookback reading the same chunk again, adds nothing.
QUARANTINE_MERGE_SQL = """
MERGE INTO {table} AS t
USING {source} AS s
ON t.file_path = s.file_path AND t.ts_utc <=> s.ts_utc AND t.reason = s.reason
WHEN NOT MATCHED THEN INSERT *
"""
QUARANTINE_COLUMNS = ["file_path", *KEY, "value", "reason", "fetched_at"]

# Values the German power system cannot produce, or the day-ahead market cannot clear, per
# hour; a quarter-hour slot holds a quarter of an hour's energy. Kept in step with
# dbt/tests/assert_values_within_physical_limits.sql, which checks silver as a backstop.
LIMITS = {
    Series.PRICE_DE_LU: (-500, 4000, False),
    Series.PRICE_DE_AT_LU: (-500, 4000, False),
    Series.LOAD: (20000, 100000, True),
    Series.WIND_ONSHORE: (0, 80000, True),
    Series.FORECAST_WIND_ONSHORE: (0, 80000, True),
    Series.WIND_OFFSHORE: (0, 20000, True),
    Series.FORECAST_WIND_OFFSHORE: (0, 20000, True),
    Series.SOLAR: (0, 150000, True),
    Series.FORECAST_SOLAR: (0, 150000, True),
}
# Forecasts and prices reach a day or two past the landing; further is a bad timestamp.
MAX_LEAD_DAYS = 3


def read_chunks(
    spark: SparkSession, root: str = LANDING_ROOT, modified_after: datetime | None = None
) -> DataFrame:
    """Chunk files under `root`/smard, landed after `modified_after` if given (UTC)."""
    reader = (
        spark.read.schema(CHUNK_SCHEMA)
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", CHUNK_GLOB)
    )
    if modified_after is not None:
        reader = reader.option("modifiedAfter", modified_after.strftime("%Y-%m-%dT%H:%M:%S"))
    return reader.json(f"{root}/smard").select(
        F.col("_metadata.file_path").alias("file_path"),
        F.col("_metadata.file_modification_time").alias("fetched_at"),
        "series",
    )


def parse_chunks(chunks: DataFrame, keep_path: bool = False) -> DataFrame:
    """One row per filled slot of each chunk.

    Expects `file_path`, `fetched_at` and `series` ([epoch_ms, value] pairs). SMARD serves
    future slots as null, so those are dropped.
    """
    path = F.col("file_path")
    return (
        chunks.select(
            F.regexp_extract(path, PATH_PATTERN, 1).try_cast("int").alias("series_id"),
            F.regexp_extract(path, PATH_PATTERN, 2).alias("region"),
            F.regexp_extract(path, PATH_PATTERN, 3).alias("resolution"),
            F.explode("series").alias("point"),
            "fetched_at",
            "file_path",
        )
        .where(F.size("point") == 2)
        .select(
            "series_id",
            "region",
            "resolution",
            F.col("point")[0].cast("long").alias("ts_ms"),
            F.col("point")[1].alias("value"),
            "fetched_at",
            *(["file_path"] if keep_path else []),
        )
        .where(F.col("value").isNotNull())
    )


def unreadable(chunks: DataFrame) -> DataFrame:
    """Quarantine rows for chunks whose JSON no longer has the expected shape, and for points
    that are not [epoch_ms, value] pairs.
    """
    path = F.col("file_path")
    keys = [
        F.regexp_extract(path, PATH_PATTERN, 1).try_cast("int").alias("series_id"),
        F.regexp_extract(path, PATH_PATTERN, 2).alias("region"),
        F.regexp_extract(path, PATH_PATTERN, 3).alias("resolution"),
    ]
    payload = chunks.where(F.col("series").isNull()).select(
        "file_path",
        *keys,
        F.lit(None).cast("timestamp").alias("ts_utc"),
        F.lit(None).cast("double").alias("value"),
        F.lit("bad_payload").alias("reason"),
        "fetched_at",
    )
    points = (
        chunks.select("file_path", *keys, F.explode("series").alias("point"), "fetched_at")
        .where(F.size("point") != 2)
        .select(
            "file_path",
            "series_id",
            "region",
            "resolution",
            F.timestamp_millis(F.col("point")[0].cast("long")).alias("ts_utc"),
            F.lit(None).cast("double").alias("value"),
            F.lit("bad_point").alias("reason"),
            "fetched_at",
        )
    )
    return payload.unionByName(points)


def with_reason(rows: DataFrame) -> DataFrame:
    """Add `reason`: null for a row fit for silver, else why it is quarantined.

    Expects the columns `with_time_columns` returns.
    """
    series, resolution, ts, value = (
        F.col("series_id"),
        F.col("resolution"),
        F.col("ts_utc"),
        F.col("value"),
    )
    step = F.when(resolution == Resolution.HOUR.value, 60).otherwise(15)
    misaligned = (F.minute(ts) % step != 0) | (F.second(ts) != 0)
    out_of_range = F.lit(False)
    for s, (low, high, per_slot) in LIMITS.items():
        scale = F.when(resolution == Resolution.QUARTERHOUR.value, 0.25 if per_slot else 1.0)
        scale = scale.otherwise(1.0)
        out_of_range = out_of_range | (
            (series == s.value) & ((value < low * scale) | (value > high * scale))
        )
    lead = F.expr(f"INTERVAL {MAX_LEAD_DAYS} DAYS")
    reason = (
        F.when(series.isNull(), "unknown_path")
        .when(~series.isin([s.value for s in Series]), "unknown_series")
        .when(~resolution.isin([r.value for r in Resolution]), "unknown_resolution")
        .when(misaligned, "misaligned_slot")
        .when(ts > F.col("fetched_at") + lead, "future_slot")
        .when(out_of_range, "out_of_range")
    )
    return rows.withColumn("reason", reason)


def split(chunks: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Rows fit for silver, and the quarantined rows with their reason."""
    rows = with_reason(with_time_columns(parse_chunks(chunks, keep_path=True)))
    good = rows.where(F.col("reason").isNull()).drop("reason", "file_path")
    bad = rows.where(F.col("reason").isNotNull()).select(*QUARANTINE_COLUMNS)
    return good, bad.unionByName(unreadable(chunks))


def with_time_columns(rows: DataFrame) -> DataFrame:
    """Replace `ts_ms` (slot start, epoch ms) with `ts_utc` and the slot's `local_date`.

    The session time zone must be UTC: `to_date` reads the shifted timestamp in it.
    """
    ts_utc = F.timestamp_millis("ts_ms")
    return rows.withColumns(
        {"ts_utc": ts_utc, "local_date": F.to_date(F.from_utc_timestamp(ts_utc, LOCAL_TZ))}
    ).drop("ts_ms")


def latest_per_slot(rows: DataFrame) -> DataFrame:
    """Keep the newest landing of each slot; a batch can hold several fetches of one chunk."""
    newest_first = Window.partitionBy(*KEY).orderBy(F.col("fetched_at").desc())
    return (
        rows.withColumn("_rank", F.row_number().over(newest_first))
        .where(F.col("_rank") == 1)
        .drop("_rank")
    )


def _changes(rows: DataFrame) -> DataFrame:
    """Landings that set a slot's value: its first, and each that differs from the one before.
    `_previous` holds the value it replaced.
    """
    by_landing = Window.partitionBy(*KEY).orderBy("fetched_at")
    return rows.withColumn("_previous", F.lag("value").over(by_landing)).where(
        F.col("_previous").isNull() | (F.col("value") != F.col("_previous"))
    )


def rebuild_rows(rows: DataFrame) -> DataFrame:
    """Silver rows from every landing of each slot, as successive MERGEs would leave them."""
    return (
        _changes(rows)
        .groupBy(*KEY)
        .agg(
            F.min("local_date").alias("local_date"),
            F.max_by("value", "fetched_at").alias("value"),
            F.min("fetched_at").alias("first_seen_at"),
            F.max("fetched_at").alias("last_changed_at"),
            (F.count("*") - 1).cast("int").alias("revision_count"),
        )
    )


def revision_history(rows: DataFrame) -> DataFrame:
    """Every revision in a set of landings, e.g. all of bronze."""
    return (
        _changes(rows)
        .where(F.col("_previous").isNotNull())
        .withColumnsRenamed(
            {"_previous": "old_value", "value": "new_value", "fetched_at": "detected_at"}
        )
        .select(*REVISION_COLUMNS)
    )


def revised(updates: DataFrame, current: DataFrame) -> DataFrame:
    """The revisions a batch makes to the current silver rows, by the same rule as MERGE."""
    s, t = updates.alias("s"), current.alias("t")
    return (
        s.join(t, KEY)
        .where(
            (F.col("s.value") != F.col("t.value"))
            & (F.col("s.fetched_at") > F.col("t.last_changed_at"))
        )
        .select(
            *KEY,
            F.col("s.local_date"),
            F.col("t.value").alias("old_value"),
            F.col("s.value").alias("new_value"),
            F.col("s.fetched_at").alias("detected_at"),
        )
    )


def log_revisions(revisions: DataFrame, table: str = REVISIONS_TABLE) -> int:
    """Append new revisions to the log, creating it on first use. Returns rows added."""
    spark = revisions.sparkSession
    spark.sql(REVISIONS_DDL.format(table=table))
    revisions.createOrReplaceTempView("smard_revisions")
    merged = spark.sql(REVISIONS_MERGE_SQL.format(table=table, source="smard_revisions"))
    return merged.first()["num_inserted_rows"]


def log_quarantine(rows: DataFrame, table: str = QUARANTINE_TABLE) -> int:
    """Add newly quarantined rows to the table, creating it on first use. Returns rows added."""
    spark = rows.sparkSession
    spark.sql(QUARANTINE_DDL.format(table=table))
    rows.withColumn("quarantined_at", F.current_timestamp()).createOrReplaceTempView(
        "smard_quarantine"
    )
    merged = spark.sql(QUARANTINE_MERGE_SQL.format(table=table, source="smard_quarantine"))
    return merged.first()["num_inserted_rows"]


def rebuild(spark: SparkSession, root: str = LANDING_ROOT, table: str = REBUILD_TABLE) -> None:
    """Replace `table` with silver replayed from every chunk in bronze."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    good, _ = split(read_chunks(spark, root))
    rows = rebuild_rows(good)
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(SILVER_DDL.format(table=table))
    rows.select(*spark.table(table).columns).writeTo(table).append()


def differences(spark: SparkSession, left: str, right: str) -> dict:
    """Slots whose value differs between two silver tables, and rows whose lineage differs."""

    def mismatched(columns: str) -> int:
        a = f"SELECT {columns} FROM {left}"
        b = f"SELECT {columns} FROM {right}"
        return spark.sql(f"({a} EXCEPT ALL {b}) UNION ALL ({b} EXCEPT ALL {a})").count()

    return {"values": mismatched(", ".join([*KEY, "value"])), "rows": mismatched("*")}


def merge_into_silver(updates: DataFrame, table: str = SILVER_TABLE) -> dict:
    """Upsert into the Delta table, creating it on first use. Returns the MERGE metrics."""
    spark = updates.sparkSession
    spark.sql(SILVER_DDL.format(table=table))
    updates.createOrReplaceTempView("smard_updates")
    return spark.sql(MERGE_SQL.format(table=table, source="smard_updates")).first().asDict()


def run(
    spark: SparkSession,
    root: str = LANDING_ROOT,
    table: str = SILVER_TABLE,
    lookback: timedelta | None = timedelta(days=3),
    revisions_table: str = REVISIONS_TABLE,
    quarantine_table: str = QUARANTINE_TABLE,
) -> dict:
    """Merge chunks landed within `lookback` (all of bronze if None) into silver, logging
    each value it changes and quarantining rows unfit for silver.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    since = datetime.now(UTC) - lookback if lookback is not None else None
    good, bad = split(read_chunks(spark, root, since))
    quarantined = log_quarantine(bad, quarantine_table)
    updates = latest_per_slot(good)
    # Logged before the MERGE overwrites the old values; a rerun after a failed MERGE
    # finds the same revisions and adds nothing.
    spark.sql(SILVER_DDL.format(table=table))
    logged = log_revisions(revised(updates, spark.table(table)), revisions_table)
    return {
        **merge_into_silver(updates, table),
        "revisions_logged": logged,
        "quarantined": quarantined,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Merge landed SMARD chunks into silver.")
    window = parser.add_mutually_exclusive_group()
    window.add_argument(
        "--lookback-days", default=3, type=int, help="chunks landed in the last N days"
    )
    window.add_argument("--all", action="store_true", help="every chunk in bronze")
    window.add_argument(
        "--rebuild", action="store_true", help="replace --table with a replay of all of bronze"
    )
    window.add_argument(
        "--revisions-from-bronze",
        action="store_true",
        help="log every revision found in all of bronze; safe to repeat",
    )
    parser.add_argument("--root", default=LANDING_ROOT)
    parser.add_argument("--table", default=SILVER_TABLE)
    parser.add_argument("--revisions-table", default=REVISIONS_TABLE)
    parser.add_argument("--compare-with", help="after --rebuild, fail if values differ from this")
    args = parser.parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    if args.revisions_from_bronze:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        good, _ = split(read_chunks(spark, args.root))
        print(f"revisions_logged={log_revisions(revision_history(good), args.revisions_table)}")
        return
    if args.rebuild:
        rebuild(spark, args.root, args.table)
        if args.compare_with:
            diffs = differences(spark, args.table, args.compare_with)
            print(f"differing values={diffs['values']} rows={diffs['rows']}")
            if diffs["values"]:
                raise SystemExit(f"{args.table} and {args.compare_with} hold different values")
        return
    lookback = None if args.all else timedelta(days=args.lookback_days)
    metrics = run(spark, args.root, args.table, lookback, args.revisions_table)
    print(" ".join(f"{k}={v}" for k, v in metrics.items()))
