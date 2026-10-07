"""Land weekly SMARD chunks in the bronze volume exactly as served."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
from datetime import UTC, date, datetime

from databricks.sdk import WorkspaceClient

from spotgrid_lakehouse.landing.volume import LANDING_ROOT, DatabricksVolume, Volume
from spotgrid_lakehouse.sources.smard import (
    Resolution,
    Series,
    SmardClient,
    chunk_name,
    default_region,
)

SOURCE = "smard"


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
    fetch_date: date,
    region: str | None = None,
) -> list[str]:
    """Fetch each chunk and write it unchanged. Returns the paths written."""
    folder = landing_dir(series, resolution, fetch_date, region)
    written = []
    for start in starts:
        path = f"{folder}/{chunk_name(series, resolution, start, region)}"
        volume.write(path, client.chunk(series, resolution, start, region))
        written.append(path)
    return written


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
    fetch_date = datetime.now(UTC).date()
    with SmardClient() as client:
        for name in args.series:
            for resolution in args.resolution:
                series = Series[name]
                starts = client.index(series, resolution)[-args.weeks :]
                written = land(client, volume, series, resolution, starts, fetch_date)
                print(f"{series.name} {resolution}: wrote {len(written)}")


if __name__ == "__main__":
    main()
