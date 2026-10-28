import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from spotgrid_lakehouse.monitoring.freshness import (
    STATE_PATH,
    SourceTracker,
    acknowledge,
    inspect_chunk,
)
from spotgrid_lakehouse.sources.smard import Resolution, Series

NOW = datetime(2026, 10, 27, 12, tzinfo=UTC)
START = int(datetime(2026, 10, 26, tzinfo=ZoneInfo("Europe/Berlin")).timestamp() * 1000)
GROUP = "4169/DE-LU/hour"
KEY = f"{GROUP}/{START}"
INDEX = f"smard/{GROUP}/_index.json"


class MemoryVolume:
    def __init__(self):
        self.files = {}

    def read(self, path):
        return self.files.get(path)

    def write(self, path, data):
        self.files[path] = data


def payload(value=80, **metadata):
    return json.dumps({"series": [[START, value], [START + 3_600_000, 90]], **metadata}).encode()


def observe(volume, raw, now=NOW):
    tracker = SourceTracker(volume, [(Series.PRICE_DE_LU, Resolution.HOUR)], now)
    tracker.observe(Series.PRICE_DE_LU, Resolution.HOUR, START, raw)
    index = json.loads(volume.read(INDEX) or b'{"chunks":{}}')
    sha = hashlib.sha256(raw).hexdigest()
    if index["chunks"].get(str(START), {}).get("sha256") != sha:
        index["chunks"][str(START)] = {
            "sha256": sha,
            "fetched_at": now.isoformat(),
            "fetch_date": now.date().isoformat(),
        }
        volume.write(INDEX, json.dumps(index).encode())
    tracker.finish_source(Series.PRICE_DE_LU, Resolution.HOUR)
    return tracker.snapshot()


def exported(now=NOW + timedelta(minutes=10)):
    return {"exported_at": now.isoformat(), "tables": {"daily_price_summary": {}}}


def established():
    volume = MemoryVolume()
    snapshot = observe(volume, payload())
    acknowledge(volume, snapshot, exported())
    return volume


def test_first_observation_does_not_invent_published_provenance():
    snapshot = observe(MemoryVolume(), payload())
    assert snapshot["freshness"]["status"] == "unknown"
    assert snapshot["freshness"]["series"][0]["unknown"] == 1


def test_semantic_hash_ignores_json_metadata_order_and_numeric_formatting():
    original = payload(100, meta_data={"updated": "earlier"})
    reformatted = (
        '{"meta_data":{"updated":"later"}, "series":'
        f"[[{START + 3_600_000},9e1],[{START},100.00]]}}"
    ).encode()
    before = inspect_chunk(original, Resolution.HOUR)
    after = inspect_chunk(reformatted, Resolution.HOUR)
    assert before["raw_sha256"] != after["raw_sha256"]
    assert before["fingerprint"] == after["fingerprint"]
    assert inspect_chunk(payload(101), Resolution.HOUR)["fingerprint"] != before["fingerprint"]


def test_published_version_is_acknowledged_only_after_export():
    volume = MemoryVolume()
    snapshot = observe(volume, payload())
    assert not json.loads(volume.read(STATE_PATH))["published"]
    snapshot = acknowledge(volume, snapshot, exported())
    assert snapshot["freshness"]["status"] == "up_to_date"
    # Source gaps are independent: up to date does not mean upstream is complete.
    assert snapshot["freshness"]["series"][0]["source_gaps"]


def test_failed_processing_stays_pending_on_unchanged_next_poll():
    volume = established()
    first = observe(volume, payload(81), NOW + timedelta(hours=1))
    second = observe(volume, payload(81), NOW + timedelta(hours=2))
    assert first["freshness"]["status"] == second["freshness"]["status"] == "pending"
    assert second["freshness"]["series"][0]["pending_age_minutes"] == 60
    assert (
        json.loads(volume.read(STATE_PATH))["published"][KEY]["fingerprint"]
        == inspect_chunk(payload(), Resolution.HOUR)["fingerprint"]
    )


def test_historical_revision_is_detected_without_advancing_latest_slot():
    volume = established()
    before = observe(volume, payload(), NOW + timedelta(hours=1))
    revised = observe(volume, payload(81), NOW + timedelta(hours=2))
    assert (
        before["freshness"]["series"][0]["latest_source_ts"]
        == (revised["freshness"]["series"][0]["latest_source_ts"])
    )
    assert revised["freshness"]["status"] == "pending"


def test_metadata_only_change_does_not_report_pending_updates():
    snapshot = observe(
        established(), payload(meta_data={"new": "metadata"}), NOW + timedelta(hours=1)
    )
    assert snapshot["freshness"]["status"] == "up_to_date"


def test_input_withdrawal_is_a_meaningful_change():
    snapshot = observe(established(), payload(None), NOW + timedelta(hours=1))
    assert snapshot["freshness"]["status"] == "pending"


def test_future_null_padding_does_not_report_pending_updates():
    volume = established()
    raw = json.loads(payload())
    raw["series"].append([START + 2 * 3_600_000, None])
    result = observe(volume, json.dumps(raw).encode(), NOW + timedelta(hours=1))
    assert result["freshness"]["status"] == "up_to_date"


def test_a_whole_missing_source_day_is_visible():
    result = observe(MemoryVolume(), payload())["freshness"]["series"][0]
    assert result["source_gaps"]["2026-10-27"] == "0/24 slots"


def test_only_the_captured_version_is_acknowledged():
    volume = established()
    captured = observe(volume, payload(81), NOW + timedelta(hours=1))
    observe(volume, payload(82), NOW + timedelta(hours=2))
    result = acknowledge(volume, captured, exported(NOW + timedelta(hours=3)))
    assert result["freshness"]["status"] == "pending"
    state = json.loads(volume.read(STATE_PATH))
    assert (
        state["published"][KEY]["fingerprint"]
        == inspect_chunk(payload(81), Resolution.HOUR)["fingerprint"]
    )
    assert (
        state["observed"][KEY]["fingerprint"]
        == inspect_chunk(payload(82), Resolution.HOUR)["fingerprint"]
    )


def test_versions_outside_silver_lookback_are_not_falsely_acknowledged():
    volume = MemoryVolume()
    snapshot = observe(volume, payload())
    result = acknowledge(volume, snapshot, exported(NOW + timedelta(days=4)))
    assert result["freshness"]["status"] == "unknown"
    assert not json.loads(volume.read(STATE_PATH))["published"]


def test_missing_precise_landing_metadata_stays_unknown():
    volume = MemoryVolume()
    tracker = SourceTracker(volume, [(Series.PRICE_DE_LU, Resolution.HOUR)], NOW)
    tracker.observe(Series.PRICE_DE_LU, Resolution.HOUR, START, payload())
    volume.write(
        INDEX,
        json.dumps(
            {
                "chunks": {
                    str(START): {
                        "sha256": hashlib.sha256(payload()).hexdigest(),
                        "fetch_date": NOW.date().isoformat(),
                    }
                }
            }
        ).encode(),
    )
    tracker.finish_source(Series.PRICE_DE_LU, Resolution.HOUR)
    snapshot = acknowledge(volume, tracker.snapshot(), exported())
    assert snapshot["freshness"]["status"] == "unknown"


def test_legacy_manifest_can_prove_recent_landing_time():
    volume = MemoryVolume()
    tracker = SourceTracker(volume, [(Series.PRICE_DE_LU, Resolution.HOUR)], NOW)
    tracker.observe(Series.PRICE_DE_LU, Resolution.HOUR, START, payload())
    sha = hashlib.sha256(payload()).hexdigest()
    volume.write(
        INDEX,
        json.dumps(
            {
                "chunks": {
                    str(START): {
                        "sha256": sha,
                        "fetch_date": NOW.date().isoformat(),
                    }
                }
            }
        ).encode(),
    )
    volume.write(
        f"smard/{GROUP}/fetch_date={NOW.date().isoformat()}/_manifest.json",
        json.dumps(
            {
                "files": {
                    "chunk.json": {
                        "chunk_start_ms": START,
                        "sha256": sha,
                        "fetched_at": NOW.isoformat(),
                    }
                },
            }
        ).encode(),
    )
    tracker.finish_source(Series.PRICE_DE_LU, Resolution.HOUR)
    assert (
        acknowledge(volume, tracker.snapshot(), exported())["freshness"]["status"] == "up_to_date"
    )


def test_pending_known_older_chunk_remains_visible_outside_current_poll_window():
    volume = established()
    observe(volume, payload(81), NOW + timedelta(hours=1))
    later_start = START + 7 * 24 * 3_600_000
    tracker = SourceTracker(
        volume, [(Series.PRICE_DE_LU, Resolution.HOUR)], NOW + timedelta(days=7)
    )
    tracker.observe(
        Series.PRICE_DE_LU,
        Resolution.HOUR,
        later_start,
        json.dumps(
            {
                "series": [[later_start, 90]],
            }
        ).encode(),
    )
    tracker.finish_source(Series.PRICE_DE_LU, Resolution.HOUR)
    row = tracker.snapshot()["freshness"]["series"][0]
    assert row["status"] == "pending" and row["pending"] == 1


@pytest.mark.parametrize(
    "raw",
    [
        b"not-json",
        b"[]",
        b'{"series":[]}',
        b'{"series":null}',
        b'{"series":[[0,NaN]]}',
        b'{"series":[[0,true]]}',
        b'{"series":[[0,"bad"]]}',
        b'{"series":[[0]]}',
        b'{"series":[[true,1]]}',
        b'{"series":[[0,1],[0,2]]}',
    ],
)
def test_bad_payloads_report_unknown_instead_of_fresh(raw):
    volume = established()
    snapshot = observe(volume, raw, NOW + timedelta(hours=1))
    assert snapshot["freshness"]["status"] == "unknown"
    assert "invalid source payload" in snapshot["freshness"]["series"][0]["error"]


def test_upstream_outage_is_unknown_even_with_previously_matching_checkpoint():
    tracker = SourceTracker(established(), [(Series.PRICE_DE_LU, Resolution.HOUR)], NOW)
    tracker.finish_source(Series.PRICE_DE_LU, Resolution.HOUR, "HTTPStatusError")
    result = tracker.snapshot()["freshness"]
    assert result["status"] == "unknown"
    assert result["series"][0]["error"] == "HTTPStatusError"


def test_corrupted_checkpoint_is_not_overwritten_with_a_fresh_baseline():
    volume = MemoryVolume()
    volume.write(STATE_PATH, b"not-json")
    result = observe(volume, payload())["freshness"]
    assert result["status"] == "unknown"
    assert "checkpoint unavailable" in result["error"]
    assert volume.read(STATE_PATH) == b"not-json"


def test_new_chunk_without_baseline_stays_pending_after_an_unrelated_export():
    volume = established()
    later = NOW + timedelta(days=7)
    next_start = START + 7 * 24 * 3_600_000
    raw = json.dumps({"series": [[next_start, 90]]}).encode()
    tracker = SourceTracker(volume, [(Series.PRICE_DE_LU, Resolution.HOUR)], later)
    tracker.observe(Series.PRICE_DE_LU, Resolution.HOUR, next_start, raw)
    index = json.loads(volume.read(INDEX))
    index["chunks"][str(next_start)] = {
        "sha256": hashlib.sha256(raw).hexdigest(),
        "fetched_at": later.isoformat(),
    }
    volume.write(INDEX, json.dumps(index).encode())
    tracker.finish_source(Series.PRICE_DE_LU, Resolution.HOUR)
    assert tracker.snapshot()["freshness"]["status"] == "pending"

    unrelated = observe(volume, payload(), NOW + timedelta(days=8))
    acknowledge(volume, unrelated, exported(NOW + timedelta(days=8, minutes=10)))
    result = observe(volume, payload(), NOW + timedelta(days=9))["freshness"]
    assert result["status"] == "pending"
    assert result["series"][0]["pending"] == 1


def test_stale_source_index_does_not_hide_the_missing_required_day():
    result = observe(MemoryVolume(), payload(), NOW + timedelta(days=10))["freshness"]
    due = (NOW + timedelta(days=10)).date().isoformat()
    assert result["series"][0]["source_gaps"][due] == "0/24 slots"


def test_checkpoint_write_failure_is_unknown_and_does_not_break_landing():
    class FailingCheckpoint(MemoryVolume):
        def write(self, path, data):
            if path == STATE_PATH:
                raise OSError("checkpoint write unavailable")
            super().write(path, data)

    snapshot = observe(FailingCheckpoint(), payload())
    assert snapshot["freshness"]["status"] == "unknown"
    assert "checkpoint write failed" in snapshot["freshness"]["error"]


@pytest.mark.parametrize("day,hours", [(date(2026, 3, 29), 23), (date(2026, 10, 25), 25)])
@pytest.mark.parametrize("resolution,factor", [(Resolution.HOUR, 1), (Resolution.QUARTERHOUR, 4)])
def test_completeness_uses_expected_dst_slots(day, hours, resolution, factor):
    start = int(
        datetime.combine(day, datetime.min.time(), ZoneInfo("Europe/Berlin")).timestamp() * 1000
    )
    slot_ms = 3_600_000 // factor
    raw = json.dumps(
        {
            "series": [[start + index * slot_ms, 1] for index in range(hours * factor)],
        }
    ).encode()
    tracker = SourceTracker(MemoryVolume(), [(Series.LOAD, resolution)], NOW)
    monday = int(
        datetime.combine(
            day - timedelta(days=6), datetime.min.time(), ZoneInfo("Europe/Berlin")
        ).timestamp()
        * 1000
    )
    tracker.observe(Series.LOAD, resolution, monday, raw)
    tracker.finish_source(Series.LOAD, resolution)
    assert day.isoformat() not in tracker.snapshot()["freshness"]["series"][0]["source_gaps"]
