"""SMARD chunks from bronze to silver time-series rows, in the DuckDB warehouse."""

from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from itertools import groupby

import duckdb
import pyarrow as pa

from spotgrid_lakehouse.landing.volume import Volume, open_volume
from spotgrid_lakehouse.sources.catalog import BY_ID, Kind

SILVER_TABLE = "silver.smard_timeseries"
REBUILD_TABLE = "silver.smard_timeseries_rebuild"
REVISIONS_TABLE = "silver.revisions"
QUARANTINE_TABLE = "silver.quarantine"
# smard/{series}/{region}/{resolution}/fetch_date=YYYY-MM-DD/{chunk}_HHMMSS.json, as landed.
PATH = re.compile(
    r"smard/(\d+)/([^/]+)/([a-z]+)/fetch_date=(\d{4}-\d{2}-\d{2})/\d[^/]*?(?:_(\d{6}))?\.json$"
)
KEY = ["series_id", "region", "resolution", "ts_utc"]
LINEAGE = ", ".join(
    [*KEY, "local_date", "value", "first_seen_at", "last_changed_at", "revision_count"]
)
SLOT_MS = {"hour": 3_600_000, "quarterhour": 900_000}
# Forecasts and prices reach a day or two past the landing; further is a bad timestamp.
MAX_LEAD_DAYS = 3
BATCH_FILES = 2000

SILVER_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  series_id INTEGER NOT NULL,
  region VARCHAR NOT NULL,
  resolution VARCHAR NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  local_date DATE NOT NULL,
  value DOUBLE NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL,
  last_changed_at TIMESTAMPTZ NOT NULL,
  revision_count INTEGER NOT NULL,
  -- When spotgrid last wrote the row; incremental gold reads changes after it.
  merged_at TIMESTAMPTZ NOT NULL
)
"""
REVISIONS_DDL = f"""
CREATE TABLE IF NOT EXISTS {REVISIONS_TABLE} (
  series_id INTEGER NOT NULL,
  region VARCHAR NOT NULL,
  resolution VARCHAR NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  local_date DATE NOT NULL,
  old_value DOUBLE NOT NULL,
  new_value DOUBLE NOT NULL,
  detected_at TIMESTAMPTZ NOT NULL
)
"""
QUARANTINE_DDL = f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
  file_path VARCHAR NOT NULL,
  series_id INTEGER,
  region VARCHAR,
  resolution VARCHAR,
  ts_utc TIMESTAMPTZ,
  value DOUBLE,
  reason VARCHAR NOT NULL,
  fetched_at TIMESTAMPTZ NOT NULL,
  quarantined_at TIMESTAMPTZ NOT NULL
)
"""

POINTS = pa.schema(
    [
        ("file_path", pa.string()),
        ("series_id", pa.int32()),
        ("region", pa.string()),
        ("resolution", pa.string()),
        ("ts_ms", pa.int64()),
        ("value", pa.float64()),
        ("fetched_at", pa.timestamp("us", tz="UTC")),
    ]
)
UNREADABLE = pa.schema(
    [
        ("file_path", pa.string()),
        ("series_id", pa.int32()),
        ("region", pa.string()),
        ("resolution", pa.string()),
        ("ts_ms", pa.int64()),
        ("reason", pa.string()),
        ("fetched_at", pa.timestamp("us", tz="UTC")),
    ]
)

# Rows fit for silver have no reason. Volume limits are Germany's, per hour; a quarter-hour
# slot holds a quarter of an hour's energy. Kept in step with
# dbt/tests/assert_values_within_physical_limits.sql, which checks silver as a backstop.
CLASSIFY_SQL = f"""
CREATE OR REPLACE TEMP TABLE classified AS
SELECT
  p.file_path, p.series_id, p.region, p.resolution,
  epoch_ms(p.ts_ms)::TIMESTAMPTZ AS ts_utc,
  timezone('Europe/Berlin', epoch_ms(p.ts_ms)::TIMESTAMPTZ)::DATE AS local_date,
  p.value, p.fetched_at,
  CASE
    WHEN p.series_id IS NULL THEN 'unknown_path'
    WHEN NOT EXISTS (SELECT 1 FROM known_series k WHERE k.series_id = p.series_id)
      THEN 'unknown_series'
    WHEN p.resolution NOT IN ('hour', 'quarterhour') THEN 'unknown_resolution'
    WHEN p.ts_ms % (CASE p.resolution WHEN 'hour' THEN 3600000 ELSE 900000 END) <> 0
      THEN 'misaligned_slot'
    WHEN epoch_ms(p.ts_ms)::TIMESTAMPTZ > p.fetched_at + INTERVAL {MAX_LEAD_DAYS} DAYS
      THEN 'future_slot'
    WHEN EXISTS (
      SELECT 1 FROM limits l
      WHERE l.series_id = p.series_id AND (l.region IS NULL OR l.region = p.region)
        AND (p.value < l.low * CASE WHEN l.per_slot AND p.resolution = 'quarterhour'
                                    THEN 0.25 ELSE 1 END
          OR p.value > l.high * CASE WHEN l.per_slot AND p.resolution = 'quarterhour'
                                     THEN 0.25 ELSE 1 END)
    ) THEN 'out_of_range'
  END AS reason
FROM points AS p
"""

# A value only changes when a newer landing disagrees, so replaying old chunks is a no-op.
MERGE_SQL = """
MERGE INTO {table} AS t
USING updates AS s
ON t.series_id = s.series_id AND t.region = s.region
  AND t.resolution = s.resolution AND t.ts_utc = s.ts_utc
WHEN MATCHED AND s.value <> t.value AND s.fetched_at > t.last_changed_at THEN UPDATE SET
  value = s.value, last_changed_at = s.fetched_at, revision_count = t.revision_count + 1,
  merged_at = now()
WHEN NOT MATCHED THEN INSERT
  (series_id, region, resolution, ts_utc, local_date, value,
   first_seen_at, last_changed_at, revision_count, merged_at)
  VALUES (s.series_id, s.region, s.resolution, s.ts_utc, s.local_date, s.value,
   s.fetched_at, s.fetched_at, 0, now())
RETURNING merge_action
"""

# The revisions a batch makes to current silver rows, by the same rule as the MERGE. Keyed on
# the landing that carried the change, so a rerun or a replay of bronze adds nothing.
LOG_REVISIONS_SQL = f"""
INSERT INTO {REVISIONS_TABLE}
SELECT s.series_id, s.region, s.resolution, s.ts_utc, s.local_date,
  t.value AS old_value, s.value AS new_value, s.fetched_at AS detected_at
FROM updates AS s
JOIN {{table}} AS t USING (series_id, region, resolution, ts_utc)
WHERE s.value <> t.value AND s.fetched_at > t.last_changed_at
  AND NOT EXISTS (
    SELECT 1 FROM {REVISIONS_TABLE} r
    WHERE r.series_id = s.series_id AND r.region = s.region AND r.resolution = s.resolution
      AND r.ts_utc = s.ts_utc AND r.detected_at = s.fetched_at
  )
"""

# A rerun, or the lookback reading the same chunk again, adds nothing.
LOG_QUARANTINE_SQL = f"""
INSERT INTO {QUARANTINE_TABLE}
SELECT b.file_path, b.series_id, b.region, b.resolution, b.ts_utc, b.value, b.reason,
  b.fetched_at, now()
FROM (
  SELECT file_path, series_id, region, resolution, ts_utc, value, reason, fetched_at
  FROM classified WHERE reason IS NOT NULL
  UNION ALL
  SELECT file_path, series_id, region, resolution,
    CASE WHEN ts_ms IS NULL THEN NULL ELSE epoch_ms(ts_ms)::TIMESTAMPTZ END,
    NULL, reason, fetched_at
  FROM unreadable
) AS b
WHERE NOT EXISTS (
  SELECT 1 FROM {QUARANTINE_TABLE} q
  WHERE q.file_path = b.file_path AND q.ts_utc IS NOT DISTINCT FROM b.ts_utc
    AND q.reason = b.reason
)
"""

LATEST_SQL = f"""
CREATE OR REPLACE TEMP TABLE updates AS
SELECT {", ".join(KEY)}, local_date, value, fetched_at
FROM classified WHERE reason IS NULL
QUALIFY row_number() OVER (PARTITION BY {", ".join(KEY)} ORDER BY fetched_at DESC) = 1
"""

# Landings that set a slot's value: its first, and each that differs from the one before.
CHANGES_SQL = f"""
CREATE OR REPLACE TEMP TABLE changes AS
SELECT * FROM (
  SELECT {", ".join(KEY)}, local_date, value, fetched_at,
    lag(value) OVER (PARTITION BY {", ".join(KEY)} ORDER BY fetched_at) AS previous
  FROM classified WHERE reason IS NULL
)
WHERE previous IS NULL OR value <> previous
"""
REBUILD_ROWS_SQL = f"""
INSERT INTO {{table}}
SELECT {", ".join(KEY)}, min(local_date), arg_max(value, fetched_at), min(fetched_at),
  max(fetched_at), (count(*) - 1)::INTEGER, now()
FROM changes GROUP BY ALL
"""
REVISION_HISTORY_SQL = f"""
INSERT INTO {REVISIONS_TABLE}
SELECT {", ".join(KEY)}, local_date, previous, value, fetched_at
FROM changes c
WHERE previous IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM {REVISIONS_TABLE} r
    WHERE r.series_id = c.series_id AND r.region = c.region AND r.resolution = c.resolution
      AND r.ts_utc = c.ts_utc AND r.detected_at = c.fetched_at
  )
"""


def connect(path: str = ":memory:", memory_limit: str | None = None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path)
    con.execute("SET TimeZone = 'UTC'")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(SILVER_DDL.format(table=SILVER_TABLE))
    con.execute(REVISIONS_DDL)
    con.execute(QUARANTINE_DDL)
    _reference_tables(con)
    return con


def _reference_tables(con: duckdb.DuckDBPyConnection) -> None:
    known = pa.table({"series_id": pa.array(sorted(BY_ID), pa.int32())})
    rows = []
    for info in BY_ID.values():
        if info.limits is None:
            continue
        low, high = info.limits
        per_slot = info.kind != Kind.PRICE
        # Prices have one zone each; volume limits hold for Germany only.
        for region in [None] if info.kind == Kind.PRICE else ["DE"]:
            rows.append((info.id, region, low, high, per_slot))
    limits = pa.table(
        {
            "series_id": pa.array([r[0] for r in rows], pa.int32()),
            "region": pa.array([r[1] for r in rows], pa.string()),
            "low": pa.array([float(r[2]) for r in rows]),
            "high": pa.array([float(r[3]) for r in rows]),
            "per_slot": pa.array([r[4] for r in rows]),
        }
    )
    con.register("known_series_arrow", known)
    con.register("limits_arrow", limits)
    con.execute("CREATE OR REPLACE TEMP TABLE known_series AS SELECT * FROM known_series_arrow")
    con.execute("CREATE OR REPLACE TEMP TABLE limits AS SELECT * FROM limits_arrow")


def fetched_at(path: str) -> datetime | None:
    """Landing time from the bronze path: the fetch date and the HHMMSS suffix, UTC."""
    match = PATH.search(path)
    if match is None:
        return None
    day, clock = match.group(4), match.group(5) or "000000"
    return datetime.strptime(f"{day}{clock}", "%Y-%m-%d%H%M%S").replace(tzinfo=UTC)


def chunk_files(volume: Volume, prefix: str = "smard/", since: date | None = None) -> list[str]:
    """Chunk files under `prefix`, landed on or after `since` if given. Manifests and indexes
    start with an underscore and are skipped.
    """
    keys = []
    for key in volume.list(prefix):
        name = key.rsplit("/", 1)[-1]
        if not name.endswith(".json") or name.startswith("_"):
            continue
        if since is not None:
            landed = re.search(r"/fetch_date=(\d{4}-\d{2}-\d{2})/", key)
            if landed and date.fromisoformat(landed.group(1)) < since:
                continue
        keys.append(key)
    return sorted(keys)


def parse(path: str, raw: bytes | None) -> tuple[list[tuple], list[tuple]]:
    """Filled slots of one chunk, and its unreadable parts. SMARD serves future slots as null,
    so those are dropped.
    """
    match = PATH.search(path)
    landed = fetched_at(path) or datetime.now(UTC)
    if match is None:
        return [], [(path, None, None, None, None, "unknown_path", landed)]
    series, region, resolution = int(match.group(1)), match.group(2), match.group(3)
    try:
        document = json.loads(raw) if raw is not None else None
    except ValueError:
        document = None
    points = document.get("series") if isinstance(document, dict) else None
    if not isinstance(points, list):
        return [], [(path, series, region, resolution, None, "bad_payload", landed)]
    good, bad = [], []
    for point in points:
        if not isinstance(point, list) or len(point) != 2:
            first = point[0] if isinstance(point, list) and point else None
            ts = first if isinstance(first, int) else None
            bad.append((path, series, region, resolution, ts, "bad_point", landed))
            continue
        ts, value = point
        if value is None:
            continue
        if not isinstance(ts, int) or not isinstance(value, int | float):
            bad.append((path, series, region, resolution, None, "bad_point", landed))
            continue
        good.append((path, series, region, resolution, ts, float(value), landed))
    return good, bad


def _table(rows: list[tuple], schema: pa.Schema) -> pa.Table:
    columns = list(zip(*rows, strict=True)) if rows else [[] for _ in schema]
    return pa.Table.from_arrays(
        [pa.array(col, field.type) for col, field in zip(columns, schema, strict=True)],
        schema=schema,
    )


def read_batch(volume: Volume, keys: list[str], workers: int = 16) -> tuple[pa.Table, pa.Table]:
    """Points and unreadable rows of these chunk files, read in parallel."""
    with ThreadPoolExecutor(workers) as pool:
        bodies = list(pool.map(volume.read, keys))
    good, bad = [], []
    for key, raw in zip(keys, bodies, strict=True):
        g, b = parse(key, raw)
        good += g
        bad += b
    return _table(good, POINTS), _table(bad, UNREADABLE)


def _stage(con: duckdb.DuckDBPyConnection, points: pa.Table, unreadable: pa.Table) -> None:
    con.register("points_arrow", points)
    con.register("unreadable_arrow", unreadable)
    con.execute("CREATE OR REPLACE TEMP TABLE points AS SELECT * FROM points_arrow")
    con.execute("CREATE OR REPLACE TEMP TABLE unreadable AS SELECT * FROM unreadable_arrow")
    con.unregister("points_arrow")
    con.unregister("unreadable_arrow")
    con.execute(CLASSIFY_SQL)


def merge_batch(
    con: duckdb.DuckDBPyConnection, points: pa.Table, unreadable: pa.Table, table: str
) -> dict:
    """Quarantine, log revisions, then upsert one batch. Revisions are logged before the MERGE
    overwrites the old values; a rerun after a failed MERGE finds the same ones and adds nothing.
    """
    _stage(con, points, unreadable)
    quarantined = con.execute(LOG_QUARANTINE_SQL).fetchone()[0]
    con.execute(LATEST_SQL)
    revisions = con.execute(LOG_REVISIONS_SQL.format(table=table)).fetchone()[0]
    actions = [row[0] for row in con.execute(MERGE_SQL.format(table=table)).fetchall()]
    return {
        "inserted": actions.count("INSERT"),
        "updated": actions.count("UPDATE"),
        "revisions_logged": revisions,
        "quarantined": quarantined,
    }


def _sum(metrics: Iterable[dict]) -> dict:
    total: dict[str, int] = {}
    for m in metrics:
        for k, v in m.items():
            total[k] = total.get(k, 0) + v
    return total


def run(
    con: duckdb.DuckDBPyConnection,
    volume: Volume,
    lookback: timedelta | None = timedelta(days=3),
    table: str = SILVER_TABLE,
    now: datetime | None = None,
) -> dict:
    """Merge chunks landed within `lookback` (all of bronze if None) into silver, logging each
    value it changes and quarantining rows unfit for silver.
    """
    con.execute(SILVER_DDL.format(table=table))
    since = ((now or datetime.now(UTC)) - lookback).date() if lookback is not None else None
    keys = chunk_files(volume, since=since)
    # A batch holds whole series, so the newest landing of a slot is always in it.
    metrics = [
        merge_batch(con, *read_batch(volume, batch), table) for batch in _group_batches(keys)
    ]
    return {"files": len(keys), **_sum(metrics)}


def _group_batches(keys: list[str]) -> Iterator[list[str]]:
    """Batches that never split one series, region and resolution."""
    batch: list[str] = []
    for _, group in groupby(keys, key=lambda k: k.split("/fetch_date=")[0]):
        files = list(group)
        if batch and len(batch) + len(files) > BATCH_FILES:
            yield batch
            batch = []
        batch += files
    if batch:
        yield batch


def rebuild(
    con: duckdb.DuckDBPyConnection,
    volume: Volume,
    table: str = REBUILD_TABLE,
    with_revisions: bool = False,
) -> dict:
    """Replace `table` with silver replayed from every chunk in bronze, as successive MERGEs
    would leave it; with `with_revisions`, also log every revision bronze holds. Works one
    series, region and resolution at a time, so memory follows the largest of them.
    """
    con.execute(f"DROP TABLE IF EXISTS {table}")
    con.execute(SILVER_DDL.format(table=table))
    keys = chunk_files(volume)
    revisions = 0
    for batch in _group_batches(keys):
        _stage(con, *read_batch(volume, batch))
        con.execute(LOG_QUARANTINE_SQL)
        con.execute(CHANGES_SQL)
        con.execute(REBUILD_ROWS_SQL.format(table=table))
        if with_revisions:
            revisions += con.execute(REVISION_HISTORY_SQL).fetchone()[0]
    rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    return {"files": len(keys), "rows": rows, "revisions_logged": revisions}


def differences(con: duckdb.DuckDBPyConnection, left: str, right: str) -> dict:
    """Slots whose value differs between two silver tables, and rows whose lineage differs."""

    def mismatched(columns: str) -> int:
        a, b = f"SELECT {columns} FROM {left}", f"SELECT {columns} FROM {right}"
        sql = f"SELECT count(*) FROM (({a} EXCEPT ALL {b}) UNION ALL ({b} EXCEPT ALL {a}))"
        return con.execute(sql).fetchone()[0]

    return {"values": mismatched(", ".join([*KEY, "value"])), "rows": mismatched(LINEAGE)}


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
    parser.add_argument("--with-revisions", action="store_true", help="with --rebuild")
    parser.add_argument("--warehouse", default=os.environ.get("WAREHOUSE", "warehouse.duckdb"))
    parser.add_argument("--bronze", default=os.environ.get("BRONZE"))
    parser.add_argument("--table", default=None)
    parser.add_argument("--compare-with", help="after --rebuild, fail if values differ from this")
    parser.add_argument("--memory-limit", default=os.environ.get("DUCKDB_MEMORY_LIMIT"))
    args = parser.parse_args(argv)
    if not args.bronze:
        parser.error("set --bronze or BRONZE")

    volume = open_volume(args.bronze)
    con = connect(args.warehouse, args.memory_limit)
    try:
        if args.rebuild:
            table = args.table or REBUILD_TABLE
            print(rebuild(con, volume, table, args.with_revisions))
            if args.compare_with:
                diffs = differences(con, table, args.compare_with)
                print(f"differing values={diffs['values']} rows={diffs['rows']}")
                if diffs["values"]:
                    raise SystemExit(f"{table} and {args.compare_with} hold different values")
            return
        lookback = None if args.all else timedelta(days=args.lookback_days)
        metrics = run(con, volume, lookback, args.table or SILVER_TABLE)
        print(" ".join(f"{k}={v}" for k, v in metrics.items()))
    finally:
        con.close()
