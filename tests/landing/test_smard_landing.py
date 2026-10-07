from datetime import date
from pathlib import Path

from spotgrid_lakehouse.landing.smard import land
from spotgrid_lakehouse.sources.smard import Resolution, Series, chunk_path

FIXTURES = Path(__file__).parents[1] / "fixtures" / "smard"
WEEK_2026_09_28 = 1790546400000
FETCH_DATE = date(2026, 10, 7)
FOLDER = "smard/4169/DE-LU/hour/fetch_date=2026-10-07"


def test_land_writes_raw_bytes_under_fetch_date(client, volume):
    written = land(
        client, volume, Series.PRICE_DE_LU, Resolution.HOUR, [WEEK_2026_09_28], FETCH_DATE
    )

    path = f"{FOLDER}/4169_DE-LU_hour_{WEEK_2026_09_28}.json"
    assert written == [path]
    expected = FIXTURES / chunk_path(Series.PRICE_DE_LU, Resolution.HOUR, WEEK_2026_09_28)
    assert volume.files[path] == expected.read_bytes()
