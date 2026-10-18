import json
import random
from datetime import UTC, datetime

import httpx

from spotgrid_lakehouse.monitoring import probe

AT = datetime(2026, 10, 18, 19, 43, 5, tzinfo=UTC)


def transport(failing: str | None = None) -> httpx.MockTransport:
    def respond(request: httpx.Request) -> httpx.Response:
        if request.url.path == failing:
            raise httpx.ConnectTimeout("slow", request=request)
        return httpx.Response(200, json={"path": request.url.path})

    return httpx.MockTransport(respond)


def test_every_endpoint_is_timed_once():
    run = probe.probe("https://api.test/", transport=transport(), rng=random.Random(1), now=AT)

    assert run["probed_at"] == "2026-10-18T19:43:05+00:00"
    assert sorted(r["path"] for r in run["results"]) == sorted(probe.PATHS)
    assert [r["position"] for r in run["results"]] == list(range(len(probe.PATHS)))
    for result in run["results"]:
        assert result["status"] == 200
        assert 0 <= result["ttfb_ms"] <= result["total_ms"]
        assert result["bytes"] == len(json.dumps({"path": result["path"]}, separators=(",", ":")))


def test_order_changes_between_runs():
    def order(seed):
        run = probe.probe("https://api.test", transport=transport(), rng=random.Random(seed))
        return tuple(r["path"] for r in run["results"])

    orders = {order(seed) for seed in range(5)}

    assert len(orders) > 1


def test_a_failed_request_is_recorded_not_raised():
    run = probe.probe("https://api.test", ["/v1/load"], transport=transport(failing="/v1/load"))

    assert run["results"] == [
        {"path": "/v1/load", "status": None, "error": "ConnectTimeout", "position": 0}
    ]


def test_summary_counts_failures_and_times_only_successes():
    def result(path, status, total):
        return {"path": path, "status": status, "total_ms": total}

    runs = [
        {"results": [result("/v1/load", 200, ms) for ms in range(1, 20)]},
        {"results": [result("/v1/load", 200, 400.0), {"path": "/v1/load", "status": None}]},
    ]

    assert probe.summary(runs) == {
        "/v1/load": {"requests": 21, "ok_pct": 95.2, "p50_ms": 10, "p95_ms": 19}
    }


def test_results_are_stored_by_utc_day():
    assert probe.key({"probed_at": AT.isoformat()}) == "probes/2026-10-18/194305.json"
