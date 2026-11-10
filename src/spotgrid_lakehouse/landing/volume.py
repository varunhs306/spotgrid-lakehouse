"""Bronze storage: S3 in production, a directory for local runs and tests."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Protocol


class Volume(Protocol):
    def read(self, path: str) -> bytes | None: ...

    def write(self, path: str, data: bytes) -> None: ...

    def list(self, prefix: str) -> Iterator[str]: ...


class S3Volume:
    """Objects under `prefix` in one bucket."""

    def __init__(self, bucket: str, prefix: str = "", client=None) -> None:
        import boto3

        self._bucket = bucket
        self._prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        self._s3 = client or boto3.client("s3")

    def read(self, path: str) -> bytes | None:
        try:
            return self._s3.get_object(Bucket=self._bucket, Key=self._prefix + path)["Body"].read()
        except self._s3.exceptions.NoSuchKey:
            return None

    def write(self, path: str, data: bytes) -> None:
        self._s3.put_object(Bucket=self._bucket, Key=self._prefix + path, Body=data)

    def list(self, prefix: str) -> Iterator[str]:
        pages = self._s3.get_paginator("list_objects_v2").paginate(
            Bucket=self._bucket, Prefix=self._prefix + prefix
        )
        for page in pages:
            for item in page.get("Contents", []):
                yield item["Key"][len(self._prefix) :]


class LocalVolume:
    """The same layout in a directory, e.g. a copy of bronze."""

    def __init__(self, root: Path) -> None:
        self._root = Path(root)

    def read(self, path: str) -> bytes | None:
        file = self._root / path
        return file.read_bytes() if file.is_file() else None

    def write(self, path: str, data: bytes) -> None:
        file = self._root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(data)

    def list(self, prefix: str) -> Iterator[str]:
        base = self._root / prefix
        start = base if base.is_dir() else base.parent
        if not start.is_dir():
            return
        for file in sorted(start.rglob("*")):
            key = file.relative_to(self._root).as_posix()
            if file.is_file() and key.startswith(prefix):
                yield key


def open_volume(location: str) -> Volume:
    """`s3://bucket/prefix` or a local directory."""
    if location.startswith("s3://"):
        bucket, _, prefix = location.removeprefix("s3://").partition("/")
        return S3Volume(bucket, prefix)
    return LocalVolume(Path(location))
