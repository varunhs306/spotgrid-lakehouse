"""Land weekly SMARD chunks in bronze exactly as served."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Callable, Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

from spotgrid_lakehouse.landing.volume import Volume, open_volume
from spotgrid_lakehouse.monitoring.freshness import SourceTracker
from spotgrid_lakehouse.sources.catalog import Kind, sources
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


def series_dir(series: int, resolution: Resolution, region: str | None = None) -> str:
    region = region or default_region(series)
    return f"{SOURCE}/{int(series)}/{region}/{resolution}"


def landing_dir(
    series: int, resolution: Resolution, fetch_date: date, region: str | None = None
) -> str:
    return f"{series_dir(series, resolution, region)}/fetch_date={fetch_date.isoformat()}"


def land(
    client: SmardClient,
    volume: Volume,
    series: int,
    resolution: Resolution,
    starts: Iterable[int],
    region: str | None = None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    observe: Callable[[int, bytes], None] | None = None,
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
        "series": int(series),
        "region": region,
        "resolution": str(resolution),
        "fetch_date": fetch_date.isoformat(),
        "files": {},
    }
    written = []
    for start in starts:
        raw = client.chunk(series, resolution, start, region)
        if observe is not None:
            observe(start, raw)
        sha256 = hashlib.sha256(raw).hexdigest()
        if index["chunks"].get(str(start), {}).get("sha256") == sha256:
            continue
        # The landing time in the name keeps every version, even two on the same day.
        name = chunk_name(series, resolution, start, region).replace(
            ".json", f"_{started:%H%M%S}.json"
        )
        volume.write(f"{folder}/{name}", raw)
        written.append(f"{folder}/{name}")
        index["chunks"][str(start)] = {
            "sha256": sha256,
            "fetch_date": fetch_date.isoformat(),
            "fetched_at": now().isoformat(),
        }
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


def monitored(series: int, region: str) -> Series | None:
    """The watched series this source is, if any: German series in their default region."""
    try:
        watched = Series(series)
    except ValueError:
        return None
    return watched if region == default_region(watched) else None


def selection(
    scope: str,
    resolutions: Iterable[Resolution],
    series_ids: Iterable[int] | None = None,
    include_ended: bool = False,
) -> list[tuple[int, str, Resolution]]:
    """(series id, region, resolution) to land: every source, or only prices. Series that
    have ended (DE-AT-LU, nuclear) publish nothing new, so only a backfill includes them.
    """
    kinds = {Kind.PRICE} if scope == "prices" else None
    wanted = set(series_ids) if series_ids else None
    return [
        (info.id, region, resolution)
        for info, region in sources(kinds)
        if (wanted is None or info.id in wanted) and (include_ended or info.ended is None)
        for resolution in resolutions
    ]


def run(
    volume: Volume,
    client: SmardClient,
    selected: list[tuple[int, str, Resolution]],
    weeks: int = 2,
    since: date | None = None,
    until: date | None = None,
    now: datetime | None = None,
) -> dict:
    """Land every selected source. A source SMARD does not publish (404) is marked
    unavailable; any other failure is raised after every source has been tried.
    """
    now = now or datetime.now(UTC)
    watched = [
        (m, resolution)
        for series, region, resolution in selected
        if (m := monitored(series, region)) is not None
    ]
    tracker = SourceTracker(volume, watched, now)
    landed, failures = [], []
    try:
        for series, region, resolution in selected:
            watch = monitored(series, region)
            entry = {
                "series": watch.name if watch else f"{series}/{region}",
                "series_id": series,
                "region": region,
                "resolution": str(resolution),
                "monitored": watch is not None,
                "checked": 0,
                "written": 0,
                "dates": None,
            }
            try:
                starts = select_chunks(
                    client.index(series, resolution, region), weeks, since, until
                )
                if since is None:
                    # A source SMARD stopped updating has no chunk this recent; skip it.
                    oldest = (now - timedelta(weeks=weeks + 1)).timestamp() * 1000
                    starts = [s for s in starts if s >= oldest]

                def observe(
                    start: int, raw: bytes, watch=watch, resolution: Resolution = resolution
                ) -> None:
                    if watch is not None:
                        tracker.observe(watch, resolution, start, raw)

                written = land(client, volume, series, resolution, starts, region, observe=observe)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code != 404:
                    failures.append(f"{entry['series']} {resolution}: {exc}")
                    entry["error"] = type(exc).__name__
                else:
                    entry["unavailable"] = True
                if watch is not None:
                    tracker.finish_source(watch, resolution, type(exc).__name__)
                landed.append(entry)
                continue
            except Exception as exc:
                failures.append(f"{entry['series']} {resolution}: {type(exc).__name__}: {exc}")
                entry["error"] = type(exc).__name__
                if watch is not None:
                    tracker.finish_source(watch, resolution, type(exc).__name__)
                landed.append(entry)
                continue
            if watch is not None:
                tracker.finish_source(watch, resolution)
            entry.update(checked=len(starts), written=len(written), dates=chunk_dates(starts))
            landed.append(entry)
    finally:
        # Even an upstream outage must leave an explicit unknown status for the report.
        snapshot = tracker.snapshot()
    report = {"landed": landed, "source_snapshot": snapshot}
    if failures:
        raise LandingFailed(report, failures)
    return report


class LandingFailed(RuntimeError):
    def __init__(self, report: dict, failures: list[str]) -> None:
        super().__init__(f"{len(failures)} sources failed: " + "; ".join(failures[:5]))
        self.report = report


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Land SMARD chunks in bronze.")
    parser.add_argument("--scope", choices=["all", "prices"], default="all")
    parser.add_argument("--series", nargs="+", type=int, help="only these SMARD series ids")
    parser.add_argument("--resolution", nargs="+", default=list(Resolution), type=Resolution)
    window = parser.add_mutually_exclusive_group()
    window.add_argument(
        "--weeks", default=2, type=int, help="latest N weekly chunks, current one included"
    )
    window.add_argument(
        "--since", type=date.fromisoformat, help="backfill: every chunk from this local day"
    )
    parser.add_argument("--until", type=date.fromisoformat, help="backfill: last local day")
    parser.add_argument(
        "--bronze", default=os.environ.get("BRONZE"), help="s3://bucket/prefix or a directory"
    )
    parser.add_argument("--report", type=Path, help="write what was landed to this JSON file")
    args = parser.parse_args(argv)
    if not args.bronze:
        parser.error("set --bronze or BRONZE")

    selected = selection(
        args.scope, args.resolution, args.series, include_ended=args.since is not None
    )
    report = None
    with SmardClient() as client:
        try:
            report = run(
                open_volume(args.bronze), client, selected, args.weeks, args.since, args.until
            )
        except LandingFailed as exc:
            report = exc.report
            raise
        finally:
            if args.report and report is not None:
                args.report.write_text(json.dumps(report, indent=2))
    for entry in report["landed"]:
        if entry["written"]:
            print(f"{entry['series']} {entry['resolution']}: wrote {entry['written']}")
    print(f"{len(report['landed'])} sources checked")


if __name__ == "__main__":
    main()
