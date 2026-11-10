import json
from datetime import UTC, datetime

import httpx
import pytest

from spotgrid_lakehouse.landing import smard as landing
from spotgrid_lakehouse.monitoring import freshness
from spotgrid_lakehouse.sources.smard import SmardClient

# Yesterday's hour, so the chunk counts as recent.
START = (int(datetime.now(UTC).timestamp()) // 3600 - 24) * 3_600_000


def test_outage_marks_the_failed_source_unknown_and_lands_the_rest(monkeypatch, tmp_path, volume):
    def upstream(request):
        if "/410/" in request.url.path:
            return httpx.Response(503)
        if "index" in request.url.path:
            return httpx.Response(200, json={"timestamps": [START]})
        return httpx.Response(200, json={"series": [[START, 80.0]]})

    client = SmardClient(
        httpx.Client(transport=httpx.MockTransport(upstream)), max_attempts=1, min_interval_s=0
    )
    monkeypatch.setattr(landing, "open_volume", lambda location: volume)
    monkeypatch.setattr(landing, "SmardClient", lambda: client)
    report = tmp_path / "landing.json"

    with pytest.raises(landing.LandingFailed, match="LOAD hour"):
        landing.main(
            ["--series", "4169", "410", "4068", "--resolution", "hour", "--bronze", "x",
             "--report", str(report)]
        )  # fmt: skip

    document = json.loads(report.read_text())
    watched = {row["series"]: row for row in document["landed"] if row["monitored"]}
    assert watched["LOAD"]["error"] == "HTTPStatusError"
    assert watched["PRICE_DE_LU"]["written"] == watched["SOLAR"]["written"] == 1
    sources = {row["series"]: row for row in document["source_snapshot"]["freshness"]["series"]}
    assert sources["LOAD"]["status"] == "unknown"
    assert sources["LOAD"]["error"] == "HTTPStatusError"
    assert sources["SOLAR"]["error"] is None


def test_successful_landing_and_publication_refresh_report_without_compute(
    monkeypatch, tmp_path, volume
):
    def upstream(request):
        if "index" in request.url.path:
            return httpx.Response(200, json={"timestamps": [START]})
        return httpx.Response(200, json={"series": [[START, 80.0]]})

    client = SmardClient(httpx.Client(transport=httpx.MockTransport(upstream)), min_interval_s=0)
    monkeypatch.setattr(landing, "open_volume", lambda location: volume)
    monkeypatch.setattr(landing, "SmardClient", lambda: client)
    report = tmp_path / "landing.json"
    landing.main(
        ["--series", "4169", "--resolution", "hour", "--bronze", "x", "--report", str(report)]
    )
    assert json.loads(report.read_text())["source_snapshot"]["freshness"]["status"] == "unknown"

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"exported_at": datetime.now(UTC).isoformat(), "tables": {}}))
    monkeypatch.setattr(freshness, "open_volume", lambda location: volume)
    freshness.main(["--landing", str(report), "--manifest", str(manifest), "--bronze", "x"])
    assert json.loads(report.read_text())["source_snapshot"]["freshness"]["status"] == "up_to_date"
