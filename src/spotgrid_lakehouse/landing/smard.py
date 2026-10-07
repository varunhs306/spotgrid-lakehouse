"""Land weekly SMARD chunks in the bronze volume exactly as served."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime

from databricks.sdk import WorkspaceClient

from spotgrid_lakehouse.landing.volume import LANDING_ROOT, DatabricksVolume, Volume
from spotgrid_lakehouse.sources.smard import (
    Resolution,
    Series,
    SmardClient,
    chunk_name,
    chunk_path,
    default_region,
)

SOURCE = "smard"
MANIFEST = "_manifest.json"
MANIFEST_VERSION = 1


def landing_dir(
    series: Series, resolution: Resolution, fetch_date: date, region: str | None = None
) -> str:
    region = region or default_region(series)
    return f"{SOURCE}/{series.value}/{region}/{resolution}/fetch_date={fetch_date.isoformat()}"


def land(
    client: SmardClient,
    volume: Volume,
    series: Series,
    resolution: Resolution,
    starts: Iterable[int],
    region: str | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[str]:
    """Fetch each chunk and write it unchanged, then record it in the folder's manifest.

    Returns the chunk paths written.
    """
    region = region or default_region(series)
    fetch_date = now().date()
    folder = landing_dir(series, resolution, fetch_date, region)
    manifest = _read_manifest(volume, folder) or {
        "version": MANIFEST_VERSION,
        "source": SOURCE,
        "series": series.value,
        "region": region,
        "resolution": str(resolution),
        "fetch_date": fetch_date.isoformat(),
        "files": {},
    }
    written = []
    for start in starts:
        raw = client.chunk(series, resolution, start, region)
        name = chunk_name(series, resolution, start, region)
        volume.write(f"{folder}/{name}", raw)
        written.append(f"{folder}/{name}")
        manifest["files"][name] = {
            "chunk_start_ms": start,
            "sha256": hashlib.sha256(raw).hexdigest(),
            "bytes": len(raw),
            "source_url": client.url(chunk_path(series, resolution, start, region)),
            "fetched_at": now().isoformat(),
        }
    # Written last, so it never lists a chunk that is missing from the folder.
    if written:
        volume.write(f"{folder}/{MANIFEST}", _dump(manifest))
    return written


def _read_manifest(volume: Volume, folder: str) -> dict | None:
    raw = volume.read(f"{folder}/{MANIFEST}")
    return json.loads(raw) if raw is not None else None


def _dump(document: dict) -> bytes:
    return json.dumps(document, indent=2, sort_keys=True).encode()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Land recent SMARD chunks in the bronze volume.")
    parser.add_argument(
        "--series", nargs="+", default=list(Series.__members__), choices=Series.__members__
    )
    parser.add_argument("--resolution", nargs="+", default=[Resolution.HOUR], type=Resolution)
    parser.add_argument(
        "--weeks", default=2, type=int, help="latest N weekly chunks, current one included"
    )
    parser.add_argument("--root", default=LANDING_ROOT)
    parser.add_argument("--profile", help="Databricks config profile; default auth chain if unset")
    args = parser.parse_args(argv)

    volume = DatabricksVolume(args.root, WorkspaceClient(profile=args.profile))
    with SmardClient() as client:
        for name in args.series:
            for resolution in args.resolution:
                series = Series[name]
                starts = client.index(series, resolution)[-args.weeks :]
                written = land(client, volume, series, resolution, starts)
                print(f"{series.name} {resolution}: wrote {len(written)}")


if __name__ == "__main__":
    main()
