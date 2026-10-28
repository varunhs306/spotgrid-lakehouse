"""Compare observed SMARD versions with published inputs using only the Files API.

The existing daily workflow is the single writer. Snapshots are private provenance,
separate from both bronze's raw-byte index and the public export manifest contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from spotgrid_lakehouse.landing.volume import DatabricksVolume, Volume
from spotgrid_lakehouse.monitoring.checks import BERLIN, horizon, hours_in_day
from spotgrid_lakehouse.sources.smard import SLOT_MS, Resolution, Series, default_region

STATE_PATH = "_monitoring/source_versions.json"
VERSION = 1


def _read(volume: Volume) -> dict:
    raw = volume.read(STATE_PATH)
    state = (
        json.loads(raw)
        if raw is not None
        else {
            "version": VERSION,
            "observed": {},
            "published": {},
        }
    )
    if state.get("version") != VERSION:
        raise ValueError("Unsupported freshness checkpoint version")
    if not isinstance(state.get("observed"), dict) or not isinstance(state.get("published"), dict):
        raise ValueError("Malformed freshness checkpoint")
    return state


def _write(volume: Volume, state: dict) -> None:
    volume.write(STATE_PATH, json.dumps(state, indent=2, sort_keys=True).encode())


def _number(value: int | Decimal) -> str:
    """Exact numeric equivalence: 100, 100.0 and 1e2 describe the same value."""
    number = Decimal(value)
    if not number.is_finite():
        raise ValueError("Non-finite value")
    if number == 0:
        return "0"
    sign, digits, exponent = number.as_tuple()
    coefficient = "".join(str(digit) for digit in digits)
    trimmed = coefficient.rstrip("0")
    exponent += len(coefficient) - len(trimmed)
    return f"{'-' if sign else ''}{trimmed}e{exponent}"


def inspect_chunk(raw: bytes, resolution: Resolution) -> dict:
    """Fingerprint meaningful content; source completeness is separate from publication lag."""
    document = json.loads(raw, parse_float=Decimal)
    points = document.get("series") if isinstance(document, dict) else None
    if not isinstance(points, list) or not points:
        raise ValueError("Missing or empty series")
    canonical, days, seen, latest = [], {}, set(), None
    for point in points:
        if not isinstance(point, list) or len(point) != 2:
            raise ValueError("Invalid point")
        ts, value = point
        if type(ts) is not int or ts in seen or ts % SLOT_MS[resolution]:
            raise ValueError("Invalid or duplicate timestamp")
        if value is not None and type(value) not in (int, Decimal):
            raise ValueError("Invalid value")
        stamp = datetime.fromtimestamp(ts / 1000, UTC)
        seen.add(ts)
        day = stamp.astimezone(BERLIN).date().isoformat()
        counts = days.setdefault(day, {"filled": 0, "slots": 0})
        counts["slots"] += 1
        if value is not None:
            # Null-only future padding creates no serving rows. A withdrawal still
            # changes this fingerprint because its previously filled slot disappears.
            canonical.append((ts, _number(value)))
            counts["filled"] += 1
            latest = max(latest if latest is not None else ts, ts)
    canonical.sort()
    fingerprint = hashlib.sha256(json.dumps(canonical, separators=(",", ":")).encode()).hexdigest()
    return {
        "fingerprint": fingerprint,
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
        "latest_source_ts": datetime.fromtimestamp(latest / 1000, UTC).isoformat()
        if latest is not None
        else None,
        "days": days,
    }


def summarize(state: dict, sources: dict, now: datetime) -> dict:
    rows = []
    for _name, source in sorted(sources.items()):
        series = Series[source["series"]]
        resolution = Resolution(source["resolution"])
        counts = {"pending": 0, "unknown": 0, "up_to_date": 0}
        days, latest, pending_since, errors = {}, None, [], set()
        for key in sorted(set(source["keys"]) | set(source.get("pending_keys", []))):
            observed = state["observed"][key]
            published = state["published"].get(key)
            if observed.get("error") or not observed.get("fingerprint"):
                status = "unknown"
                if observed.get("error"):
                    errors.add(observed["error"])
            elif published:
                status = (
                    "up_to_date"
                    if observed["fingerprint"] == published["fingerprint"]
                    else "pending"
                )
            else:
                # An older export has no trustworthy source provenance. Do not seed a
                # "fresh" baseline just because landing succeeded or the API answers 200.
                status = "pending" if observed.get("unpublished") else "unknown"
            counts[status] += 1
            if status == "pending":
                pending_since.append(observed["pending_since"])
            if not observed.get("error"):
                if timestamp := observed.get("latest_source_ts"):
                    latest = max(latest or timestamp, timestamp)
                for day, entry in observed.get("days", {}).items():
                    days[day] = days.get(day, 0) + entry["filled"]
        if source.get("error") or not source["keys"]:
            status = "unknown"
        else:
            status = (
                "pending"
                if counts["pending"]
                else ("unknown" if counts["unknown"] else "up_to_date")
            )
        due = horizon(series, now).isoformat()
        if series != Series.PRICE_DE_AT_LU:
            # A stale upstream index can omit the newest required day altogether.
            days.setdefault(due, 0)
        factor = 4 if resolution == Resolution.QUARTERHOUR else 1
        short = {
            day: f"{filled}/{hours_in_day(datetime.fromisoformat(day).date()) * factor} slots"
            for day, filled in sorted(days.items())
            if day <= due and filled < hours_in_day(datetime.fromisoformat(day).date()) * factor
        }
        oldest = min(pending_since) if pending_since else None
        rows.append(
            {
                "series": series.name,
                "resolution": str(resolution),
                "status": status,
                "checked_chunks": len(source["keys"]),
                **counts,
                "latest_source_ts": latest,
                "source_gaps": short,
                "pending_age_minutes": (
                    max(0, int((now - datetime.fromisoformat(oldest)).total_seconds() // 60))
                    if oldest
                    else None
                ),
                "error": source.get("error") or "; ".join(sorted(errors)) or None,
                "historical": series == Series.PRICE_DE_AT_LU,
            }
        )
    active = [row for row in rows if not row["historical"]]
    status = (
        "unknown"
        if not active or state.get("error")
        else (
            "pending"
            if any(row["status"] == "pending" for row in active)
            else ("unknown" if any(row["status"] == "unknown" for row in active) else "up_to_date")
        )
    )
    return {
        "status": status,
        "checked_at": now.isoformat(),
        "series": rows,
        "last_exported_at": state.get("last_exported_at"),
        "error": state.get("error"),
    }


class SourceTracker:
    def __init__(self, volume: Volume, selections: list[tuple[Series, Resolution]], now: datetime):
        self.volume, self.now = volume, now
        self.read_failed = False
        try:
            self.state = _read(volume)
        except Exception as exc:
            self.read_failed = True
            self.state = {
                "version": VERSION,
                "observed": {},
                "published": {},
                "error": f"checkpoint unavailable: {type(exc).__name__}",
            }
        self.sources = {
            f"{series.value}/{default_region(series)}/{resolution}": {
                "series": series.name,
                "resolution": str(resolution),
                "keys": [],
                "error": "not checked",
                "pending_keys": [],
            }
            for series, resolution in selections
        }
        for key, observed in self.state["observed"].items():
            published = self.state["published"].get(key)
            group = key.rsplit("/", 1)[0]
            if group in self.sources and (
                (published and observed.get("fingerprint") != published["fingerprint"])
                or (not published and observed.get("unpublished"))
            ):
                self.sources[group]["pending_keys"].append(key)

    def observe(self, series: Series, resolution: Resolution, start: int, raw: bytes) -> None:
        group = f"{series.value}/{default_region(series)}/{resolution}"
        key = f"{group}/{start}"
        self.sources[group]["keys"].append(key)
        previous = self.state["observed"].get(key, {})
        try:
            entry = inspect_chunk(raw, resolution)
            first = datetime.fromtimestamp(start / 1000, BERLIN).date()
            for offset in range(7):
                day = (first + timedelta(days=offset)).isoformat()
                entry["days"].setdefault(day, {"filled": 0, "slots": 0})
        except (ValueError, TypeError, OverflowError, OSError) as exc:
            entry = {"error": f"invalid source payload: {type(exc).__name__}"}
        same = entry.get("fingerprint") and entry.get("fingerprint") == previous.get("fingerprint")
        entry["first_observed_at"] = previous["first_observed_at"] if same else self.now.isoformat()
        published = self.state["published"].get(key)
        was_pending = (
            published and previous.get("fingerprint") != published["fingerprint"]
        ) or previous.get("unpublished")
        entry["unpublished"] = bool(previous.get("unpublished"))
        entry["pending_since"] = (
            previous.get("pending_since", self.now.isoformat())
            if was_pending
            else self.now.isoformat()
        )
        self.state["observed"][key] = entry

    def finish_source(
        self, series: Series, resolution: Resolution, error: str | None = None
    ) -> None:
        group = f"{series.value}/{default_region(series)}/{resolution}"
        source = self.sources[group]
        source["error"] = error
        if error:
            return
        try:
            raw = self.volume.read(f"smard/{group}/_index.json")
            index = json.loads(raw)["chunks"] if raw else {}
            manifests = {}
            for key in source["keys"]:
                entry = self.state["observed"][key]
                chunk = index.get(key.rsplit("/", 1)[1], {})
                if chunk.get("sha256") != entry.get("raw_sha256"):
                    continue
                landed_at = chunk.get("fetched_at")
                if not landed_at and chunk.get("fetch_date"):
                    folder = f"smard/{group}/fetch_date={chunk['fetch_date']}"
                    if folder not in manifests:
                        body = self.volume.read(f"{folder}/_manifest.json")
                        manifests[folder] = json.loads(body).get("files", {}) if body else {}
                    matches = [
                        file["fetched_at"]
                        for file in manifests[folder].values()
                        if str(file.get("chunk_start_ms")) == key.rsplit("/", 1)[1]
                        and file.get("sha256") == entry["raw_sha256"]
                    ]
                    landed_at = max(matches) if matches else None
                entry["landed_at"] = landed_at
                last_export = self.state.get("last_exported_at")
                if (
                    landed_at
                    and last_export
                    and (
                        datetime.fromisoformat(landed_at) > datetime.fromisoformat(last_export)
                        and datetime.fromisoformat(entry["first_observed_at"])
                        > datetime.fromisoformat(last_export)
                    )
                ):
                    entry["unpublished"] = True
        except Exception as exc:
            source["error"] = f"landing metadata unavailable: {type(exc).__name__}"

    def snapshot(self) -> dict:
        if not self.read_failed:
            try:
                _write(self.volume, self.state)
            except Exception as exc:
                self.state["error"] = f"checkpoint write failed: {type(exc).__name__}"
        keys = {key for source in self.sources.values() for key in source["keys"]}
        return {
            "version": VERSION,
            "sources": self.sources,
            "observed": {key: self.state["observed"][key] for key in sorted(keys)},
            "freshness": summarize(self.state, self.sources, self.now),
        }


def acknowledge(volume: Volume, snapshot: dict, manifest: dict) -> dict:
    """Acknowledge captured inputs only after successful export and proven recent landing.

    The unchanged silver job reads files modified in the last three days. Using export
    time as the cutoff is conservative: it cannot acknowledge files older than the job's
    lookback. Legacy input versions without precise landing metadata stay unverified.
    """
    if snapshot.get("version") != VERSION or snapshot["freshness"].get("error"):
        raise ValueError("Cannot acknowledge an invalid source snapshot")
    exported_at = datetime.fromisoformat(manifest["exported_at"])
    if exported_at.tzinfo is None:
        raise ValueError("Export time must include a timezone")
    state = _read(volume)
    if state.get("last_exported_at") and (
        datetime.fromisoformat(state["last_exported_at"]) > exported_at
    ):
        raise ValueError("Cannot acknowledge an older export")
    cutoff = exported_at - timedelta(days=3)
    for key, observed in snapshot["observed"].items():
        landed = observed.get("landed_at")
        if observed.get("error") or not landed or not observed.get("fingerprint"):
            continue
        landed_at = datetime.fromisoformat(landed)
        if landed_at.tzinfo is None or not cutoff <= landed_at <= exported_at:
            continue
        state["published"][key] = {
            "fingerprint": observed["fingerprint"],
            "exported_at": manifest["exported_at"],
        }
        current = state["observed"].get(key, {})
        if current.get("fingerprint") == observed["fingerprint"]:
            current["pending_since"] = exported_at.isoformat()
            current["unpublished"] = False
    state["last_exported_at"] = manifest["exported_at"]
    _write(volume, state)
    snapshot["freshness"] = summarize(state, snapshot["sources"], exported_at)
    return snapshot


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Acknowledge inputs after a successful export.")
    parser.add_argument("--landing", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    args = parser.parse_args(argv)
    document = json.loads(args.landing.read_text())
    document["source_snapshot"] = acknowledge(
        DatabricksVolume(),
        document["source_snapshot"],
        json.loads(args.manifest.read_text()),
    )
    args.landing.write_text(json.dumps(document, indent=2))


if __name__ == "__main__":
    main()
