"""Export gold tables from the SQL warehouse to the serving bucket as Parquet.

The public API reads these files, so it never wakes the warehouse.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Protocol
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

GOLD_SCHEMA = "workspace.gold"
# Table -> its time column, which orders the rows.
TABLES = {"hourly_features": "ts_utc", "daily_price_summary": "local_date"}
PREFIX = "gold"
PARQUET = "application/vnd.apache.parquet"
MANIFEST_KEY = "_manifest.json"
MANIFEST_VERSION = 1
# CC BY 4.0 asks for the source, a licence link and a note that the data was changed.
ATTRIBUTION = (
    "Bundesnetzagentur | SMARD.de, CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/); "
    "aggregated and derived by spotgrid-lakehouse"
)


class Warehouse(Protocol):
    def read(self, table: str, order_by: str) -> pa.Table: ...


class Store(Protocol):
    def put(self, key: str, data: bytes, content_type: str) -> None: ...


def parquet_key(table: str) -> str:
    return f"{PREFIX}/{table}.parquet"


def to_parquet(table: pa.Table) -> bytes:
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue()


def export(
    warehouse: Warehouse,
    store: Store,
    tables: dict[str, str] = TABLES,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict:
    """Write each table to its fixed key, then the manifest that describes them.

    The manifest goes last: readers that start from it never see an export half written.
    """
    entries = {}
    for name, time_column in tables.items():
        data = warehouse.read(name, time_column)
        body = to_parquet(data)
        key = parquet_key(name)
        store.put(key, body, PARQUET)
        latest = pc.max(data[time_column]).as_py() if data.num_rows else None
        entries[name] = {
            "key": key,
            "rows": data.num_rows,
            "time_column": time_column,
            "max_time": latest.isoformat() if latest is not None else None,
            "sha256": hashlib.sha256(body).hexdigest(),
            "bytes": len(body),
        }
    manifest = {
        "version": MANIFEST_VERSION,
        "exported_at": now().isoformat(),
        "attribution": ATTRIBUTION,
        "tables": entries,
    }
    store.put(MANIFEST_KEY, json.dumps(manifest, indent=2).encode(), "application/json")
    return manifest


class SqlWarehouse:
    """Gold tables through a Databricks SQL warehouse; auth follows the SDK's default chain."""

    def __init__(self, http_path: str, profile: str | None = None) -> None:
        from databricks import sql
        from databricks.sdk.core import Config

        config = Config(profile=profile)
        self._connection = sql.connect(
            server_hostname=urlparse(config.host).netloc,
            http_path=http_path,
            credentials_provider=lambda: config.authenticate,
        )

    def read(self, table: str, order_by: str) -> pa.Table:
        with self._connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {GOLD_SCHEMA}.{table} ORDER BY {order_by}")
            return cursor.fetchall_arrow()

    def close(self) -> None:
        self._connection.close()


class S3Store:
    def __init__(self, bucket: str) -> None:
        import boto3

        self._bucket = bucket
        self._s3 = boto3.client("s3")

    def put(self, key: str, data: bytes, content_type: str) -> None:
        self._s3.put_object(Bucket=self._bucket, Key=key, Body=data, ContentType=content_type)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Export gold tables to the serving bucket.")
    parser.add_argument("--bucket", default=os.environ.get("SERVING_BUCKET"))
    parser.add_argument("--http-path", default=os.environ.get("DATABRICKS_HTTP_PATH"))
    parser.add_argument("--profile", help="Databricks config profile; default auth chain if unset")
    args = parser.parse_args(argv)
    if not args.bucket or not args.http_path:
        parser.error("set --bucket and --http-path, or SERVING_BUCKET and DATABRICKS_HTTP_PATH")

    warehouse = SqlWarehouse(args.http_path, args.profile)
    try:
        manifest = export(warehouse, S3Store(args.bucket))
    finally:
        warehouse.close()
    for name, entry in manifest["tables"].items():
        print(f"{name}: {entry['rows']} rows up to {entry['max_time']} -> {entry['key']}")


if __name__ == "__main__":
    main()
