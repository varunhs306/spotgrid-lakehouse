"""Export gold tables from the SQL warehouse to the serving bucket as Parquet.

The public API reads these files, so it never wakes the warehouse.
"""

from __future__ import annotations

import argparse
import io
import os
from typing import Protocol
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq

GOLD_SCHEMA = "workspace.gold"
# Table -> its time column, which orders the rows.
TABLES = {"hourly_features": "ts_utc", "daily_price_summary": "local_date"}
PREFIX = "gold"
PARQUET = "application/vnd.apache.parquet"


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


def export(warehouse: Warehouse, store: Store, tables: dict[str, str] = TABLES) -> list[str]:
    """Write each table to its fixed key, replacing the previous export."""
    keys = []
    for name, time_column in tables.items():
        key = parquet_key(name)
        store.put(key, to_parquet(warehouse.read(name, time_column)), PARQUET)
        keys.append(key)
    return keys


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
        for key in export(warehouse, S3Store(args.bucket)):
            print(f"wrote s3://{args.bucket}/{key}")
    finally:
        warehouse.close()


if __name__ == "__main__":
    main()
