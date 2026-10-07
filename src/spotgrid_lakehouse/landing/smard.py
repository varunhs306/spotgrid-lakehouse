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
INDEX = "_index.json"
MANIFEST_VERSION = 1
INDEX_VERSION = 1


def series_dir(series: Series, resolution: Resolution, region: str | None = None) -> str:
    region = region or default_region(series)
    return f"{SOURCE}/{series.value}/{region}/{resolution}"


def landing_dir(
    series: Series, resolution: Resolution, fetch_date: date, region: str | None = None
) -> str:
    return f"{series_dir(series, resolution, region)}/fetch_date={fetch_date.isoformat()}"


def land(
    client: SmardClient,
    volume: Volume,
    series: Series,
    resolution: Resolution,
    starts: Iterable[int],
    region: str | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> list[str]:
    """Fetch each chunk and write it unchanged, unless its bytes have landed before.

    Returns the chunk paths written; an unchanged rerun writes nothing.
    """
    region = region or default_region(series)
    fetch_date = now().date()
    folder = landing_dir(series, resolution, fetch_date, region)
    index_path = f"{series_dir(series, resolution, region)}/{INDEX}"
    index = _read_json(volume, index_path) or {"version": INDEX_VERSION, "chunks": {}}
    manifest = _read_json(volume, f"{folder}/{MANIFEST}") or {
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
        sha256 = hashlib.sha256(raw).hexdigest()
        if index["chunks"].get(str(start), {}).get("sha256") == sha256:
            continue
        name = chunk_name(series, resolution, start, region)
        volume.write(f"{folder}/{name}", raw)
        written.append(f"{folder}/{name}")
        index["chunks"][str(start)] = {"sha256": sha256, "fetch_date": fetch_date.isoformat()}
        manifest["files"][name] = {
            "chunk_start_ms": start,
            "sha256": sha256,
            "bytes": len(raw),
            "source_url": client.url(chunk_path(series, resolution, start, region)),
            "fetched_at": now().isoformat(),
        }
    # Chunks, then manifest, then index: a run that dies midway is redone in full next time,
    # because nothing counts as landed until the index says so.
    if written:
        volume.write(f"{folder}/{MANIFEST}", _dump(manifest))
        volume.write(index_path, _dump(index))
    return written


def _read_json(volume: Volume, path: str) -> dict | None:
    raw = volume.read(path)
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
                skipped = len(starts) - len(written)
                print(f"{series.name} {resolution}: wrote {len(written)}, skipped {skipped}")


if __name__ == "__main__":
    main()
