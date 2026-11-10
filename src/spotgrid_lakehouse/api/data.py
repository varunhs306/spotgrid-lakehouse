"""The gold export as the API sees it: the manifest and the Parquet partitions a request needs,
cached between requests.
"""

from __future__ import annotations

import io
import json
import time
from collections import OrderedDict
from collections.abc import Callable, Iterable
from datetime import date
from typing import Protocol

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from spotgrid_lakehouse.export.gold import MANIFEST_KEY

# A warm container keeps about this much Parquet decoded in memory.
CACHE_BYTES = 192 * 1024 * 1024


class NoExport(Exception):
    """The serving bucket holds no export yet, or not of this table."""


class Source(Protocol):
    def get(self, key: str) -> bytes | None: ...


class GoldData:
    """Re-reads the manifest at most once per `ttl` seconds; a partition is downloaded again
    only when the manifest lists a new checksum for it.
    """

    def __init__(
        self,
        source: Source,
        ttl: float = 600,
        clock: Callable[[], float] = time.monotonic,
        cache_bytes: int = CACHE_BYTES,
    ) -> None:
        self._source = source
        self._ttl = ttl
        self._clock = clock
        self._cache_bytes = cache_bytes
        self._manifest: dict | None = None
        self._checked_at = 0.0
        self._parts: OrderedDict[str, tuple[str, pa.Table]] = OrderedDict()

    def manifest(self, refresh: bool = False) -> dict:
        if refresh or self._manifest is None or self._clock() - self._checked_at >= self._ttl:
            raw = self._source.get(MANIFEST_KEY)
            if raw is None:
                raise NoExport(MANIFEST_KEY)
            self._manifest = json.loads(raw)
            self._checked_at = self._clock()
        return self._manifest

    def entry(self, table: str) -> dict:
        entry = self.manifest()["tables"].get(table)
        if entry is None:
            raise NoExport(table)
        return entry

    def columns(self, table: str) -> list[str]:
        return [c["name"] for c in self.entry(table)["columns"]]

    def _partition(self, part: dict) -> pa.Table:
        cached = self._parts.get(part["key"])
        if cached is not None and cached[0] == part["sha256"]:
            self._parts.move_to_end(part["key"])
            return cached[1]
        raw = self._source.get(part["key"])
        if raw is None:
            raise NoExport(part["key"])
        table = pq.read_table(io.BytesIO(raw))
        self._parts[part["key"]] = (part["sha256"], table)
        while sum(t.nbytes for _, t in self._parts.values()) > self._cache_bytes:
            self._parts.popitem(last=False)
        return table

    def read(
        self,
        table: str,
        areas: Iterable[str] | None,
        first: date | None = None,
        last: date | None = None,
    ) -> pa.Table:
        """Rows of `areas` (all for a table without areas) whose date lies in first..last."""
        entry = self.entry(table)
        wanted = set(areas) if areas is not None else None
        parts = []
        for name, part in sorted(entry["partitions"].items()):
            area, _, year = name.rpartition("/")
            if name != "all":
                if wanted is not None and area not in wanted:
                    continue
                if first is not None and int(year) < first.year:
                    continue
                if last is not None and int(year) > last.year:
                    continue
            parts.append(self._partition(part))
        if not parts:
            return pa.table({c: pa.array([], pa.null()) for c in self.columns(table)}).slice(0, 0)
        data = pa.concat_tables(parts, promote_options="permissive")
        return data

    def latest(self, table: str, areas: Iterable[str]) -> date | None:
        """Last date of `areas` the export holds, from the manifest alone."""
        entry = self.entry(table)
        years = [
            int(name.rpartition("/")[2])
            for name in entry["partitions"]
            if name.rpartition("/")[0] in set(areas)
        ]
        if not years:
            return None
        recent = self.read(table, areas, date(max(years), 1, 1), date(max(years), 12, 31))
        column = "month" if "month" in recent.column_names else "local_date"
        return pc.max(recent[column]).as_py() if recent.num_rows else None

    def warm(self, tables: Iterable[tuple[str, str]]) -> None:
        """Re-read the manifest and load the newest partition of each (table, area)."""
        manifest = self.manifest(refresh=True)
        for table, area in tables:
            parts = {
                n: p
                for n, p in manifest["tables"].get(table, {}).get("partitions", {}).items()
                if n.rpartition("/")[0] == area
            }
            if parts:
                self._partition(parts[max(parts)])


class S3Source:
    def __init__(self, bucket: str, client=None) -> None:
        import boto3

        self.bucket = bucket
        self.s3 = client or boto3.client("s3")

    def get(self, key: str) -> bytes | None:
        try:
            return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            return None

    def url(self, key: str, seconds: int = 3600) -> str:
        """A download link for a bulk file, valid for `seconds`."""
        return self.s3.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=seconds
        )
