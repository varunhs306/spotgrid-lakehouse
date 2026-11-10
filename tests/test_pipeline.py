"""A whole pipeline run on local directories, against a fake SMARD serving every source."""

import json
from datetime import UTC, datetime

import httpx
import pytest

from spotgrid_lakehouse import pipeline
from spotgrid_lakehouse.landing import smard as landing
from spotgrid_lakehouse.sources.smard import SmardClient

# A week of slots ending today, so the chunks count as recent.
WEEK = (int(datetime.now(UTC).timestamp()) // 86400 - 7) * 86_400_000


def smard(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if "/251/" in path:  # DE-AT-LU: SMARD's index ends in 2018; treat it as unpublished here
        return httpx.Response(404)
    if path.endswith(f"index_{'quarterhour' if 'quarterhour' in path else 'hour'}.json"):
        return httpx.Response(200, json={"timestamps": [WEEK]})
    series_id = int(path.split("/")[3])
    quarter = "quarterhour" in path
    step, slots = (900_000, 672) if quarter else (3_600_000, 168)
    region = path.split("/")[4]
    if series_id in (
        4169,
        251,
        5078,
        4170,
        4996,
        4997,
        252,
        253,
        254,
        255,
        256,
        259,
        260,
        261,
        262,
    ):
        value = 80.0
    elif series_id == 410:
        value = (30000.0 if region == "DE" else 5000.0) / (4 if quarter else 1)
    else:
        value = 500.0 / (4 if quarter else 1)
    return httpx.Response(200, json={"series": [[WEEK + i * step, value] for i in range(slots)]})


@pytest.fixture
def env(tmp_path, monkeypatch):
    def client():
        http = httpx.Client(transport=httpx.MockTransport(smard))
        return SmardClient(http, base_url="https://smard.test/app/chart_data", min_interval_s=0)

    monkeypatch.setattr(pipeline, "SmardClient", client)
    monkeypatch.setattr(landing, "SmardClient", client)
    return {
        "LAKE": str(tmp_path / "lake"),
        "BRONZE": str(tmp_path / "lake" / "bronze"),
        "SERVING": str(tmp_path / "serving"),
    }


def test_a_run_lands_merges_builds_and_exports(env, tmp_path, capsys):
    assert pipeline.run("morning", env) == 0

    manifest = json.loads((tmp_path / "serving" / "_manifest.json").read_text())
    assert manifest["tables"]["grid_hourly"]["rows"] == 168 * 9
    assert manifest["tables"]["prices_quarterhour"]["rows"] == 672 * 14
    assert (tmp_path / "lake" / "warehouse.duckdb").exists()
    assert not (tmp_path / "lake" / "run.lock").exists()
    out = capsys.readouterr().out
    assert "| export | ✅ |" in out
    assert "PRICE_DE_AT_LU" not in out  # ended in 2018, not fetched

    # A poll with nothing new stops after landing.
    assert pipeline.run("poll", env) == 0
    out = capsys.readouterr().out
    assert "| merge silver | ⏭️ |" in out


def test_a_held_lock_skips_the_run(env, tmp_path):
    lock = tmp_path / "lake" / "run.lock"
    lock.parent.mkdir(parents=True)
    lock.write_text(json.dumps({"since": datetime.now(UTC).isoformat()}))

    assert pipeline.run("morning", env) == 0
    assert not (tmp_path / "serving").exists()


def test_a_stale_lock_is_taken_over(env, tmp_path):
    lock = tmp_path / "lake" / "run.lock"
    lock.parent.mkdir(parents=True)
    lock.write_text(json.dumps({"since": "2026-01-01T00:00:00+00:00"}))

    assert pipeline.run("poll", env) == 0
    assert (tmp_path / "serving" / "_manifest.json").exists()


def test_new_data_rules():
    landed = {"landed": [{"series": "LOAD", "written": 1}, {"series": "PRICE_DE_LU", "written": 0}]}

    assert pipeline.new_data(landed, "morning")
    assert not pipeline.new_data(landed, "poll")
    assert pipeline.new_data(None, "manual")
    assert not pipeline.new_data(None, "morning")
