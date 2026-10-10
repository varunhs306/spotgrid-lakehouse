"""The gold export as the API sees it: manifest and Parquet tables, cached between requests."""

from __future__ import annotations

import io
import json
import time
from collections.abc import Callable
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from spotgrid_lakehouse.export.gold import MANIFEST_KEY


class NoExport(Exception):
    """The serving bucket holds no export yet."""


class Source(Protocol):
    def get(self, key: str) -> bytes | None: ...


class GoldData:
    """Re-reads the manifest at most once per `ttl` seconds; a table is downloaded again only
    when the manifest lists a new checksum for it. A warm Lambda serves from memory.
    """

    def __init__(
        self,
        source: Source,
        ttl: float = 300,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._source = source
        self._ttl = ttl
        self._clock = clock
        self._manifest: dict | None = None
        self._checked_at = 0.0
        self._tables: dict[str, tuple[str, pa.Table]] = {}

    def manifest(self) -> dict:
        if self._manifest is None or self._clock() - self._checked_at >= self._ttl:
            raw = self._source.get(MANIFEST_KEY)
            if raw is None:
                raise NoExport(MANIFEST_KEY)
            self._manifest = json.loads(raw)
            self._checked_at = self._clock()
        return self._manifest

    def table(self, name: str) -> pa.Table:
        entry = self.manifest()["tables"][name]
        cached = self._tables.get(name)
        if cached is not None and cached[0] == entry["sha256"]:
            return cached[1]
        raw = self._source.get(entry["key"])
        if raw is None:
            raise NoExport(entry["key"])
        table = pq.read_table(io.BytesIO(raw))
        self._tables[name] = (entry["sha256"], table)
        return table


class S3Source:
    def __init__(self, bucket: str) -> None:
        import boto3

        self._bucket = bucket
        self._s3 = boto3.client("s3")

    def get(self, key: str) -> bytes | None:
        try:
            return self._s3.get_object(Bucket=self._bucket, Key=key)["Body"].read()
        except self._s3.exceptions.NoSuchKey:
            return None
