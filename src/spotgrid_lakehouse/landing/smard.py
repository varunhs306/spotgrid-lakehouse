"""Land weekly SMARD chunks in the bronze volume exactly as served."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

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
BERLIN = ZoneInfo("Europe/Berlin")


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
    started = now()
    fetch_date = started.date()
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
        # The landing time in the name keeps every version, even two on the same day.
        name = chunk_name(series, resolution, start, region).replace(
            ".json", f"_{started:%H%M%S}.json"
        )
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


def local_week_start(ms: int) -> date:
    """Local Berlin day a weekly chunk starts on; chunks start at Monday midnight."""
    return datetime.fromtimestamp(ms / 1000, BERLIN).date()


def select_chunks(
    starts: list[int], weeks: int = 2, since: date | None = None, until: date | None = None
) -> list[int]:
    """The latest `weeks` chunks, or with `since`, every chunk whose week overlaps since..until."""
    if since is None:
        return starts[-weeks:]
    return [
        s
        for s in starts
        if local_week_start(s) + timedelta(days=6) >= since
        and (until is None or local_week_start(s) <= until)
    ]


def chunk_dates(starts: list[int]) -> tuple[str, str] | None:
    """First and last local day covered by weekly chunks starting at `starts` (epoch ms)."""
    if not starts:
        return None
    first, last = local_week_start(min(starts)), local_week_start(max(starts))
    return first.isoformat(), (last + timedelta(days=6)).isoformat()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Land SMARD chunks in the bronze volume.")
    parser.add_argument(
        "--series", nargs="+", default=list(Series.__members__), choices=Series.__members__
    )
    parser.add_argument("--resolution", nargs="+", default=[Resolution.HOUR], type=Resolution)
    window = parser.add_mutually_exclusive_group()
    window.add_argument(
        "--weeks", default=2, type=int, help="latest N weekly chunks, current one included"
    )
    window.add_argument(
        "--since", type=date.fromisoformat, help="backfill: every chunk from this local day"
    )
    parser.add_argument("--until", type=date.fromisoformat, help="backfill: last local day")
    parser.add_argument("--root", default=LANDING_ROOT)
    parser.add_argument("--profile", help="Databricks config profile; default auth chain if unset")
    parser.add_argument("--report", type=Path, help="write what was landed to this JSON file")
    args = parser.parse_args(argv)

    volume = DatabricksVolume(args.root, WorkspaceClient(profile=args.profile))
    landed = []
    with SmardClient() as client:
        for name in args.series:
            for resolution in args.resolution:
                series = Series[name]
                starts = select_chunks(
                    client.index(series, resolution), args.weeks, args.since, args.until
                )
                written = land(client, volume, series, resolution, starts)
                skipped = len(starts) - len(written)
                print(f"{series.name} {resolution}: wrote {len(written)}, skipped {skipped}")
                landed.append(
                    {
                        "series": series.name,
                        "series_id": series.value,
                        "resolution": str(resolution),
                        "checked": len(starts),
                        "written": len(written),
                        "dates": chunk_dates(starts),
                    }
                )
    if args.report:
        args.report.write_text(json.dumps({"landed": landed}, indent=2))


if __name__ == "__main__":
    main()
