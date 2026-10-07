import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from spotgrid_lakehouse.landing.smard import land
from spotgrid_lakehouse.sources.smard import Resolution, Series, chunk_path

FIXTURES = Path(__file__).parents[1] / "fixtures" / "smard"
WEEK_2026_09_28 = 1790546400000
NOW = datetime(2026, 10, 7, 5, 30, tzinfo=UTC)
FOLDER = "smard/4169/DE-LU/hour/fetch_date=2026-10-07"
NAME = f"4169_DE-LU_hour_{WEEK_2026_09_28}.json"
RAW = (FIXTURES / chunk_path(Series.PRICE_DE_LU, Resolution.HOUR, WEEK_2026_09_28)).read_bytes()


def land_price_week(client, volume, now=NOW):
    return land(
        client, volume, Series.PRICE_DE_LU, Resolution.HOUR, [WEEK_2026_09_28], now=lambda: now
    )


def test_land_writes_raw_bytes_under_fetch_date(client, volume):
    assert land_price_week(client, volume) == [f"{FOLDER}/{NAME}"]
    assert volume.files[f"{FOLDER}/{NAME}"] == RAW


def test_manifest_records_checksum_and_source_url(client, volume):
    land_price_week(client, volume)

    manifest = json.loads(volume.files[f"{FOLDER}/_manifest.json"])
    assert manifest["series"] == 4169
    assert manifest["region"] == "DE-LU"
    assert manifest["fetch_date"] == "2026-10-07"
    assert manifest["files"][NAME] == {
        "chunk_start_ms": WEEK_2026_09_28,
        "sha256": hashlib.sha256(RAW).hexdigest(),
        "bytes": len(RAW),
        "source_url": f"https://smard.test/app/chart_data/4169/DE-LU/{NAME}",
        "fetched_at": "2026-10-07T05:30:00+00:00",
    }


def test_manifest_is_written_after_the_chunks(client, volume):
    land_price_week(client, volume)
    assert volume.writes[-1] == f"{FOLDER}/_manifest.json"


def test_same_day_rerun_keeps_earlier_manifest_entries(client, volume):
    earlier = {"version": 1, "files": {"earlier.json": {"sha256": "abc"}}}
    volume.files[f"{FOLDER}/_manifest.json"] = json.dumps(earlier).encode()

    land_price_week(client, volume)

    manifest = json.loads(volume.files[f"{FOLDER}/_manifest.json"])
    assert sorted(manifest["files"]) == sorted(["earlier.json", NAME])
