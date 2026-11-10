import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

from spotgrid_lakehouse.landing.smard import chunk_dates, land, select_chunks
from spotgrid_lakehouse.sources.smard import Resolution, Series, chunk_path

FIXTURES = Path(__file__).parents[1] / "fixtures" / "smard"
WEEK_2026_09_28 = 1790546400000
NOW = datetime(2026, 10, 7, 5, 30, tzinfo=UTC)
FOLDER = "smard/4169/DE-LU/hour/fetch_date=2026-10-07"
NAME = f"4169_DE-LU_hour_{WEEK_2026_09_28}_053000.json"
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
        "source_url": f"https://smard.test/app/chart_data/4169/DE-LU/4169_DE-LU_hour_{WEEK_2026_09_28}.json",
        "fetched_at": "2026-10-07T05:30:00+00:00",
    }


def test_writes_chunks_then_manifest_then_index(client, volume):
    land_price_week(client, volume)
    assert volume.writes == [
        f"{FOLDER}/{NAME}",
        f"{FOLDER}/_manifest.json",
        "smard/4169/DE-LU/hour/_index.json",
    ]


def test_same_day_rerun_keeps_earlier_manifest_entries(client, volume):
    earlier = {"version": 1, "files": {"earlier.json": {"sha256": "abc"}}}
    volume.files[f"{FOLDER}/_manifest.json"] = json.dumps(earlier).encode()

    land_price_week(client, volume)

    manifest = json.loads(volume.files[f"{FOLDER}/_manifest.json"])
    assert sorted(manifest["files"]) == sorted(["earlier.json", NAME])


def test_chunk_dates_span_local_weeks():
    # Weekly chunks start at local midnight on Monday: 2026-09-28 and 2026-10-05.
    assert chunk_dates([1791151200000, WEEK_2026_09_28]) == ("2026-09-28", "2026-10-11")
    assert chunk_dates([]) is None


WEEK_2026_10_05 = 1791151200000
WEEK_2026_10_12 = 1791756000000


def test_select_latest_weeks():
    starts = [WEEK_2026_09_28, WEEK_2026_10_05, WEEK_2026_10_12]
    assert select_chunks(starts, weeks=2) == [WEEK_2026_10_05, WEEK_2026_10_12]


def test_select_weeks_overlapping_a_date_range():
    starts = [WEEK_2026_09_28, WEEK_2026_10_05, WEEK_2026_10_12]
    # Sunday 4 Oct is the last day of the first week; Monday 5 Oct starts the second.
    assert select_chunks(starts, since=date(2026, 10, 4), until=date(2026, 10, 5)) == [
        WEEK_2026_09_28,
        WEEK_2026_10_05,
    ]
    assert select_chunks(starts, since=date(2026, 10, 5)) == [WEEK_2026_10_05, WEEK_2026_10_12]


def test_ended_series_are_left_out_except_for_a_backfill():
    from spotgrid_lakehouse.landing.smard import selection

    daily = {(s, r) for s, r, _ in selection("all", [Resolution.HOUR])}
    backfill = {(s, r) for s, r, _ in selection("all", [Resolution.HOUR], include_ended=True)}

    ended = {(251, "DE-AT-LU"), (1224, "DE"), (1224, "Amprion"), (1224, "TenneT"),
             (1224, "TransnetBW")}  # fmt: skip
    assert backfill - daily == ended


def test_a_source_smard_stopped_updating_is_not_fetched(volume):
    import httpx

    from spotgrid_lakehouse.landing.smard import run
    from spotgrid_lakehouse.sources.smard import SmardClient

    requests = []

    def stale(request):
        requests.append(request.url.path)
        return httpx.Response(200, json={"timestamps": [WEEK_2026_09_28]})

    client = SmardClient(httpx.Client(transport=httpx.MockTransport(stale)), min_interval_s=0)
    report = run(
        volume, client, [(410, "AT", Resolution.HOUR)], now=datetime(2026, 11, 11, tzinfo=UTC)
    )

    assert report["landed"][0]["checked"] == 0
    assert len(requests) == 1  # the index only
