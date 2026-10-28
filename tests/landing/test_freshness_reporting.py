import json
from datetime import UTC, datetime

import httpx
import pytest

from spotgrid_lakehouse.landing import smard as landing
from spotgrid_lakehouse.monitoring import freshness
from spotgrid_lakehouse.sources.smard import SmardClient

START = 1790546400000


def test_outage_writes_unknown_report_for_failed_and_unchecked_sources(
    monkeypatch, tmp_path, volume
):
    def upstream(request):
        if "/410/" in request.url.path:
            return httpx.Response(503)
        if "index" in request.url.path:
            return httpx.Response(200, json={"timestamps": [START]})
        return httpx.Response(200, json={"series": [[START, 80.0]]})

    client = SmardClient(
        httpx.Client(transport=httpx.MockTransport(upstream)), max_attempts=1, min_interval_s=0
    )
    monkeypatch.setattr(landing, "WorkspaceClient", lambda **kwargs: None)
    monkeypatch.setattr(landing, "DatabricksVolume", lambda *args: volume)
    monkeypatch.setattr(landing, "SmardClient", lambda: client)
    report = tmp_path / "landing.json"

    with pytest.raises(httpx.HTTPStatusError):
        landing.main(
            [
                "--series",
                "PRICE_DE_LU",
                "LOAD",
                "SOLAR",
                "--resolution",
                "hour",
                "--report",
                str(report),
            ]
        )

    document = json.loads(report.read_text())
    assert [row["series"] for row in document["landed"]] == ["PRICE_DE_LU"]
    sources = {row["series"]: row for row in document["source_snapshot"]["freshness"]["series"]}
    assert sources["LOAD"]["status"] == sources["SOLAR"]["status"] == "unknown"
    assert sources["LOAD"]["error"] == "HTTPStatusError"
    assert sources["SOLAR"]["error"] == "not checked"


def test_successful_landing_and_publication_refresh_report_without_compute(
    monkeypatch, tmp_path, volume
):
    def upstream(request):
        if "index" in request.url.path:
            return httpx.Response(200, json={"timestamps": [START]})
        return httpx.Response(200, json={"series": [[START, 80.0]]})

    client = SmardClient(httpx.Client(transport=httpx.MockTransport(upstream)), min_interval_s=0)
    monkeypatch.setattr(landing, "WorkspaceClient", lambda **kwargs: None)
    monkeypatch.setattr(landing, "DatabricksVolume", lambda *args: volume)
    monkeypatch.setattr(landing, "SmardClient", lambda: client)
    report = tmp_path / "landing.json"
    landing.main(["--series", "PRICE_DE_LU", "--resolution", "hour", "--report", str(report)])
    assert json.loads(report.read_text())["source_snapshot"]["freshness"]["status"] == "unknown"

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"exported_at": datetime.now(UTC).isoformat(), "tables": {}}))
    monkeypatch.setattr(freshness, "DatabricksVolume", lambda: volume)
    freshness.main(["--landing", str(report), "--manifest", str(manifest)])
    assert json.loads(report.read_text())["source_snapshot"]["freshness"]["status"] == "up_to_date"
