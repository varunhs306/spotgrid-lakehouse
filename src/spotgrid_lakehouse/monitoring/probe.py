"""Times the public API the way a visitor sees it: through its public domain, a new connection
per request. Runs as a scheduled Lambda; a failed endpoint fails the invocation, which alarms.

`run` probes each endpoint once, in random order so no endpoint always comes first after a quiet
spell, and stores the result in the serving bucket. `summary` gives p50 and p95 per endpoint.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

import httpx

PATHS = (
    "/v2/grid",
    "/v2/grid?resolution=day",
    "/v2/prices",
    "/v2/prices?resolution=day",
    "/v2/catalog",
)
PREFIX = "probes/"


def time_request(base_url: str, path: str, transport: httpx.BaseTransport | None = None) -> dict:
    """Time to first byte and to the last, both including DNS, TCP and TLS."""
    started = time.perf_counter()
    try:
        with (
            httpx.Client(base_url=base_url, timeout=20, transport=transport) as client,
            client.stream("GET", path) as response,
        ):
            first_byte = time.perf_counter()
            size = len(response.read())
    except httpx.HTTPError as error:
        return {"path": path, "status": None, "error": type(error).__name__}
    finished = time.perf_counter()
    return {
        "path": path,
        "status": response.status_code,
        "ttfb_ms": round(1000 * (first_byte - started), 1),
        "total_ms": round(1000 * (finished - started), 1),
        "bytes": size,
    }


def probe(
    base_url: str,
    paths: Iterable[str] = PATHS,
    transport: httpx.BaseTransport | None = None,
    rng: random.Random | None = None,
    now: datetime | None = None,
) -> dict:
    order = list(paths)
    (rng or random.Random()).shuffle(order)
    results = [
        {**time_request(base_url, path, transport), "position": position}
        for position, path in enumerate(order)
    ]
    return {"probed_at": (now or datetime.now(UTC)).isoformat(), "results": results}


def percentile(values: list[float], share: float) -> float:
    """Nearest rank: the smallest value with at least `share` of the values at or below it."""
    ranked = sorted(values)
    return ranked[max(0, math.ceil(len(ranked) * share) - 1)]


def summary(runs: Iterable[dict]) -> dict[str, dict]:
    by_path: dict[str, list[dict]] = {}
    for run in runs:
        for result in run["results"]:
            by_path.setdefault(result["path"], []).append(result)
    out = {}
    for path, results in sorted(by_path.items()):
        ok = [r["total_ms"] for r in results if r["status"] == 200]
        out[path] = {
            "requests": len(results),
            "ok_pct": round(100 * len(ok) / len(results), 1),
            "p50_ms": percentile(ok, 0.5) if ok else None,
            "p95_ms": percentile(ok, 0.95) if ok else None,
        }
    return out


def key(run: dict) -> str:
    at = datetime.fromisoformat(run["probed_at"])
    return f"{PREFIX}{at:%Y-%m-%d}/{at:%H%M%S}.json"


def stored_runs(bucket: str, days: int, now: datetime | None = None) -> list[dict]:
    import boto3

    s3 = boto3.client("s3")
    today = (now or datetime.now(UTC)).date()
    runs = []
    for back in range(days):
        prefix = f"{PREFIX}{today - timedelta(days=back):%Y-%m-%d}/"
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                body = s3.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()
                runs.append(json.loads(body))
    return runs


def run_and_store(url: str, bucket: str | None) -> dict:
    result = probe(url)
    if bucket:
        import boto3

        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=key(result),
            Body=json.dumps(result).encode(),
            ContentType="application/json",
        )
    return result


def handler(event: dict, context: object) -> dict:
    """Lambda entrypoint: probe API_URL, keep the result in SERVING_BUCKET."""
    import os

    result = run_and_store(os.environ["API_URL"], os.environ.get("SERVING_BUCKET"))
    failed = [r for r in result["results"] if r["status"] != 200]
    if failed:
        raise RuntimeError(f"endpoints failed: {failed}")
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m spotgrid_lakehouse.monitoring.probe")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="probe every endpoint once")
    run.add_argument("--url", required=True)
    run.add_argument("--bucket", help="store the result here")
    show = commands.add_parser("summary", help="p50 and p95 per endpoint")
    source = show.add_mutually_exclusive_group(required=True)
    source.add_argument("--bucket")
    source.add_argument("--file", help="JSON lines, one run each, as `run` prints them")
    show.add_argument("--days", type=int, default=7)
    args = parser.parse_args(argv)

    if args.command == "run":
        result = run_and_store(args.url, args.bucket)
        print(json.dumps(result))
        return 0 if all(r["status"] == 200 for r in result["results"]) else 1

    if args.file:
        with open(args.file, encoding="utf-8") as lines:
            runs = [json.loads(line) for line in lines if line.strip()]
    else:
        runs = stored_runs(args.bucket, args.days)
    for path, stats in summary(runs).items():
        print(path, " ".join(f"{name}={value}" for name, value in stats.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
