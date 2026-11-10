"""Export gold from the warehouse to the serving bucket.

Slot tables go out as Parquet per area and year, plus CSV per area and month for bulk
download; only partitions with rows changed since the last export are rewritten. The API
reads these files, never the warehouse.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

if TYPE_CHECKING:
    import duckdb

PARQUET = "application/vnd.apache.parquet"
CSV = "text/csv; charset=utf-8"
MANIFEST_KEY = "_manifest.json"
# Bumped on any change readers must handle; contracts/manifest.schema.json describes it.
# 3: tables are partitioned by area and year, with monthly CSV for bulk download.
MANIFEST_VERSION = 3
# CC BY 4.0 asks for the source, a licence link and a note that the data was changed.
ATTRIBUTION = (
    "Bundesnetzagentur | SMARD.de, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/); "
    "aggregated and derived by spotgrid-lakehouse"
)


@dataclass(frozen=True)
class Table:
    name: str
    time_column: str
    area_column: str | None = None
    # Date column a partition's year, and a bulk file's month, come from.
    date_column: str | None = None
    bulk: bool = False


TABLES = (
    Table("grid_quarterhour", "ts_utc", "region", "local_date", bulk=True),
    Table("grid_hourly", "ts_utc", "region", "local_date", bulk=True),
    Table("grid_daily", "local_date", "region", "local_date"),
    Table("grid_monthly", "month", "region", "month"),
    Table("prices_quarterhour", "ts_utc", "zone", "local_date", bulk=True),
    Table("prices_hourly", "ts_utc", "zone", "local_date", bulk=True),
    Table("prices_daily", "local_date", "zone", "local_date"),
    Table("prices_monthly", "month", "zone", "month"),
    Table("revisions", "detected_at", None, "local_date"),
    Table("revision_stats", "last_revision_at"),
    Table("coverage", "last_changed_at"),
)
BY_NAME = {t.name: t for t in TABLES}


class Store(Protocol):
    def put(self, key: str, data: bytes, content_type: str) -> None: ...

    def get(self, key: str) -> bytes | None: ...


def parquet_key(table: str, area: str | None = None, year: int | None = None) -> str:
    if year is None:
        return f"gold/{table}.parquet"
    return f"gold/{table}/{area}/{year}.parquet"


def bulk_key(table: str, area: str, month: str) -> str:
    return f"bulk/{table}/{area}/{month}.csv"


def to_parquet(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue()


def to_csv(table: pa.Table) -> bytes:
    sink = io.StringIO()
    writer = csv.writer(sink, lineterminator="\n")
    writer.writerow(table.column_names)
    for row in table.to_pylist():
        writer.writerow(cell(v) for v in row.values())
    return sink.getvalue().encode()


def cell(value) -> str:
    """CSV text for one value: UTC times end in Z, booleans are lower case, null is empty."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _iso(value) -> str | None:
    return value.isoformat() if isinstance(value, date | datetime) else value


def _entry(key: str, body: bytes, data: pa.Table, t: Table) -> dict:
    latest = pc.max(data[t.time_column]).as_py() if data.num_rows else None
    return {
        "key": key,
        "rows": data.num_rows,
        "max_time": _iso(latest),
        "sha256": hashlib.sha256(body).hexdigest(),
        "bytes": len(body),
    }


def _columns(con: duckdb.DuckDBPyConnection, t: Table) -> list[dict]:
    schema = con.execute(f"SELECT * FROM gold.{t.name} LIMIT 0").to_arrow_table().schema
    return [{"name": f.name, "type": str(f.type)} for f in schema]


def _groups(
    con: duckdb.DuckDBPyConnection, t: Table, part: str, since: datetime | None
) -> list[tuple]:
    """(area, part) groups of `t`; with `since`, only those with rows updated after it."""
    area = t.area_column or "'all'"
    updated = since is not None and any(c["name"] == "updated_at" for c in _columns(con, t))
    where = "WHERE updated_at > ?" if updated else ""
    sql = f"SELECT DISTINCT {area}, {part} FROM gold.{t.name} {where} ORDER BY ALL"
    return con.execute(sql, [since] if updated else []).fetchall()


def _slice(con: duckdb.DuckDBPyConnection, t: Table, area: str, part: str, value) -> pa.Table:
    where = f"{part} = ?" + (f" AND {t.area_column} = ?" if t.area_column else "")
    params = [value, area] if t.area_column else [value]
    sql = f"SELECT * FROM gold.{t.name} WHERE {where} ORDER BY {t.time_column}"
    return con.execute(sql, params).to_arrow_table()


def previous_manifest(store: Store) -> dict | None:
    raw = store.get(MANIFEST_KEY)
    if raw is None:
        return None
    manifest = json.loads(raw)
    return manifest if manifest.get("version") == MANIFEST_VERSION else None


def export(
    con: duckdb.DuckDBPyConnection,
    store: Store,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    full: bool = False,
) -> dict:
    """Write changed partitions, then the manifest that describes every partition.

    The manifest goes last: readers that start from it never see an export half written.
    """
    started = now()
    previous = None if full else previous_manifest(store)
    since = datetime.fromisoformat(previous["updated_through"]) if previous else None
    updated_through = con.execute(
        "SELECT max(updated_at) FROM gold.grid_hourly UNION ALL "
        "SELECT max(updated_at) FROM gold.prices_hourly"
    ).fetchall()
    entries = {}
    for t in TABLES:
        old = (previous or {}).get("tables", {}).get(t.name, {})
        entry = {
            "time_column": t.time_column,
            "area_column": t.area_column,
            "columns": _columns(con, t),
            "partitions": dict(old.get("partitions", {})),
            "bulk": dict(old.get("bulk", {})),
        }
        if t.date_column is None:
            data = con.execute(f"SELECT * FROM gold.{t.name} ORDER BY ALL").to_arrow_table()
            body = to_parquet(data)
            key = parquet_key(t.name)
            store.put(key, body, PARQUET)
            entry["partitions"] = {"all": _entry(key, body, data, t)}
        else:
            year = f"year({t.date_column})"
            for area, value in _groups(con, t, year, since):
                data = _slice(con, t, area, year, value)
                body = to_parquet(data)
                key = parquet_key(t.name, area, value)
                store.put(key, body, PARQUET)
                entry["partitions"][f"{area}/{value}"] = _entry(key, body, data, t)
            if t.bulk:
                month = f"strftime({t.date_column}, '%Y-%m')"
                for area, value in _groups(con, t, month, since):
                    data = _slice(con, t, area, month, value)
                    body = to_csv(data)
                    key = bulk_key(t.name, area, value)
                    store.put(key, body, CSV)
                    entry["bulk"][f"{area}/{value}"] = _entry(key, body, data, t)
        entry["rows"] = sum(p["rows"] for p in entry["partitions"].values())
        times = [p["max_time"] for p in entry["partitions"].values() if p["max_time"]]
        entry["max_time"] = max(times) if times else None
        entries[t.name] = entry
    latest = max((r[0] for r in updated_through if r[0] is not None), default=None)
    manifest = {
        "version": MANIFEST_VERSION,
        "exported_at": started.isoformat(),
        "updated_through": _iso(latest) or started.isoformat(),
        "attribution": ATTRIBUTION,
        "tables": entries,
    }
    store.put(MANIFEST_KEY, json.dumps(manifest, indent=2).encode(), "application/json")
    return manifest


def snapshot(con: duckdb.DuckDBPyConnection, out: Path, now: datetime | None = None) -> dict:
    """Every gold table as one Parquet file, with a manifest and SHA256SUMS: a release."""
    out.mkdir(parents=True, exist_ok=True)
    files = {}
    for t in TABLES:
        order = f"{t.area_column}, {t.time_column}" if t.area_column else "ALL"
        data = con.execute(f"SELECT * FROM gold.{t.name} ORDER BY {order}").to_arrow_table()
        body = to_parquet(data)
        (out / f"{t.name}.parquet").write_bytes(body)
        files[f"{t.name}.parquet"] = {
            "rows": data.num_rows,
            "sha256": hashlib.sha256(body).hexdigest(),
            "columns": [{"name": f.name, "type": str(f.type)} for f in data.schema],
        }
    manifest = {
        "exported_at": (now or datetime.now(UTC)).isoformat(),
        "attribution": ATTRIBUTION,
        "files": files,
    }
    body = json.dumps(manifest, indent=2).encode()
    (out / "manifest.json").write_bytes(body)
    sums = {**{n: f["sha256"] for n, f in files.items()}, "manifest.json": _sha(body)}
    (out / "SHA256SUMS").write_text("".join(f"{h}  {n}\n" for n, h in sorted(sums.items())))
    return manifest


def _sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


class S3Store:
    def __init__(self, bucket: str, client=None) -> None:
        import boto3

        self._bucket = bucket
        self._s3 = client or boto3.client("s3")

    def put(self, key: str, data: bytes, content_type: str) -> None:
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=data, ContentType=content_type)

    def get(self, key: str) -> bytes | None:
        try:
            return self._s3.get_object(Bucket=self._bucket, Key=key)["Body"].read()
        except self._s3.exceptions.NoSuchKey:
            return None


class LocalStore:
    """A directory laid out like the serving bucket."""

    def __init__(self, root: Path) -> None:
        self._root = Path(root)

    def put(self, key: str, data: bytes, content_type: str) -> None:
        path = self._root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def get(self, key: str) -> bytes | None:
        path = self._root / key
        return path.read_bytes() if path.is_file() else None


def open_store(location: str) -> Store:
    """`s3://bucket` or a local directory."""
    if location.startswith("s3://"):
        return S3Store(location.removeprefix("s3://").strip("/"))
    return LocalStore(Path(location))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Export gold to the serving bucket.")
    parser.add_argument("--serving", default=os.environ.get("SERVING"), help="s3://bucket or dir")
    parser.add_argument("--warehouse", default=os.environ.get("WAREHOUSE", "warehouse.duckdb"))
    parser.add_argument("--full", action="store_true", help="rewrite every partition")
    parser.add_argument("--report", type=Path, help="also write the manifest to this file")
    args = parser.parse_args(argv)
    if not args.serving:
        parser.error("set --serving or SERVING")

    import duckdb

    con = duckdb.connect(args.warehouse, read_only=True)
    con.execute("SET TimeZone = 'UTC'")
    try:
        manifest = export(con, open_store(args.serving), full=args.full)
    finally:
        con.close()
    for name, entry in manifest["tables"].items():
        print(f"{name}: {entry['rows']} rows up to {entry['max_time']}")
    if args.report:
        args.report.write_text(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
