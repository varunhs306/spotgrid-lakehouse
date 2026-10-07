"""Unity Catalog volume access from outside the workspace (Free Edition blocks outbound calls)."""

from __future__ import annotations

import io
from typing import Protocol

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

LANDING_ROOT = "/Volumes/workspace/bronze/landing"


class Volume(Protocol):
    def read(self, path: str) -> bytes | None: ...

    def write(self, path: str, data: bytes) -> None: ...


class DatabricksVolume:
    """Files under a volume root, through the Databricks Files API."""

    def __init__(self, root: str = LANDING_ROOT, client: WorkspaceClient | None = None) -> None:
        self._root = root.rstrip("/")
        self._files = (client or WorkspaceClient()).files

    def read(self, path: str) -> bytes | None:
        try:
            return self._files.download(f"{self._root}/{path}").contents.read()
        except NotFound:
            return None

    def write(self, path: str, data: bytes) -> None:
        self._files.upload(f"{self._root}/{path}", io.BytesIO(data), overwrite=True)
