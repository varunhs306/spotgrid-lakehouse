"""Record SMARD responses for offline tests: uv run python scripts/record_smard_fixtures.py"""

from pathlib import Path

from spotgrid_lakehouse.sources.smard import (
    Resolution,
    Series,
    SmardClient,
    chunk_path,
    index_path,
)

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "smard"

WEEK_2026_09_28 = 1790546400000  # Monday 00:00 Europe/Berlin
WEEK_2026_10_05 = 1791151200000  # partly in the future when recorded, so it ends in nulls
LAST_DE_AT_LU_WEEK = 1537740000000  # the zone ended 2018-09-30 24:00 Europe/Berlin

PATHS = [
    index_path(Series.PRICE_DE_LU, Resolution.HOUR),
    chunk_path(Series.PRICE_DE_LU, Resolution.HOUR, WEEK_2026_09_28),
    chunk_path(Series.PRICE_DE_LU, Resolution.QUARTERHOUR, WEEK_2026_09_28),
    chunk_path(Series.LOAD, Resolution.HOUR, WEEK_2026_10_05),
    chunk_path(Series.PRICE_DE_AT_LU, Resolution.HOUR, LAST_DE_AT_LU_WEEK),
]


def main() -> None:
    with SmardClient() as client:
        for path in PATHS:
            content = client.get_raw(path).content
            target = OUT / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            print(f"{path} {len(content)} bytes")


if __name__ == "__main__":
    main()
