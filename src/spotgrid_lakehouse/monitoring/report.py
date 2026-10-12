"""What one daily run did, collected from each step, and rendered for the Actions summary.

Collectors never raise for a missing input: a step that failed leaves a gap in the report,
not a second failure.
"""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from spotgrid_lakehouse.monitoring.checks import (
    WINDOW_DAYS,
    Finding,
    missing_hours,
    price_revisions,
)
from spotgrid_lakehouse.sources.smard import Series

SILVER = "workspace.silver.smard_timeseries"
# Step names in .github/workflows/daily.yml, in run order.
STEPS = ("land bronze", "merge silver", "build gold", "export")
ICONS = {"success": "✅", "failure": "❌", "cancelled": "⛔", "skipped": "⏭️"}

# Rows the silver MERGE touched in this run: first landed, or changed by a newer landing.
SILVER_CHANGES_SQL = f"""
SELECT series_id, local_date,
  count_if(first_seen_at >= :started_at) AS inserted,
  count_if(first_seen_at < :started_at) AS revised
FROM {SILVER}
WHERE resolution = 'hour' AND last_changed_at >= :started_at
GROUP BY series_id, local_date
"""
COVERAGE_SQL = f"""
SELECT series_id, local_date, count(*) AS hours
FROM {SILVER}
WHERE resolution = 'hour' AND local_date >= :first_day
GROUP BY series_id, local_date
"""


def read_json(path: Path | None) -> Any:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text())


def dbt_summary(results: dict | None) -> dict | None:
    """Counts from dbt's run_results.json: models and tests that passed, failed or were skipped."""
    if results is None:
        return None
    counts = {"passed": 0, "failed": 0, "warned": 0, "skipped": 0}
    failed = []
    for result in results.get("results", []):
        status = result["status"]
        if status in ("success", "pass"):
            counts["passed"] += 1
        elif status == "warn":
            counts["warned"] += 1
        elif status == "skipped":
            counts["skipped"] += 1
        else:
            counts["failed"] += 1
            failed.append(result["unique_id"].split(".")[-1])
    return {**counts, "failed_nodes": failed}


def silver_summary(changes: list[dict]) -> list[dict]:
    totals: dict[int, dict] = {}
    for row in changes:
        entry = totals.setdefault(row["series_id"], {"inserted": 0, "revised": 0})
        entry["inserted"] += row["inserted"]
        entry["revised"] += row["revised"]
    return [
        {"series": Series(series_id).name, **counts} for series_id, counts in sorted(totals.items())
    ]


def check_api(url: str | None, http: httpx.Client | None = None) -> dict | None:
    """The public freshness endpoint, as a reviewer would see it."""
    if not url:
        return None
    http = http or httpx.Client(timeout=30)
    started = time.monotonic()
    try:
        response = http.get(f"{url.rstrip('/')}/v1/freshness")
    except httpx.HTTPError as exc:
        return {"ok": False, "error": type(exc).__name__}
    body = response.json() if response.status_code == 200 else {}
    return {
        "ok": response.status_code == 200,
        "status": response.status_code,
        "ms": round((time.monotonic() - started) * 1000),
        "age_minutes": body.get("age_minutes"),
    }


def build(
    *,
    run: dict,
    started_at: datetime,
    now: datetime,
    outcomes: dict[str, str],
    seconds: dict[str, int],
    landing: dict | None,
    silver_changes: list[dict] | None,
    coverage: list[dict] | None,
    dbt: dict | None,
    manifest: dict | None,
    api: dict | None,
) -> dict:
    steps = [
        {"name": name, "outcome": outcomes.get(name, "skipped"), "seconds": seconds.get(name)}
        for name in STEPS
    ]
    failed = next((s["name"] for s in steps if s["outcome"] in ("failure", "cancelled")), None)
    if failed is None and api is not None and not api["ok"]:
        failed = "API check"

    findings: list[Finding] = []
    if coverage is not None:
        rows = [(r["series_id"], r["local_date"], r["hours"]) for r in coverage]
        findings += missing_hours(rows, now)
    if silver_changes is not None:
        findings += price_revisions(
            (r["series_id"], r["local_date"], r["revised"]) for r in silver_changes
        )

    return {
        "run": {
            **run,
            "started_at": started_at.isoformat(),
            "seconds": int((now - started_at).total_seconds()),
            "status": "failure" if failed else "success",
            "failed_step": failed,
        },
        "steps": steps,
        "landing": landing["landed"] if landing else None,
        "silver": silver_summary(silver_changes) if silver_changes is not None else None,
        "dbt": dbt,
        "export": manifest,
        "api": api,
        "findings": [asdict(f) for f in findings],
    }


def duration(seconds: int | None) -> str:
    if seconds is None:
        return "-"
    minutes, rest = divmod(seconds, 60)
    return f"{minutes}m{rest:02d}s" if minutes else f"{rest}s"


def markdown(report: dict, issues: dict[str, str] | None = None) -> str:
    """The Actions run summary. `issues` maps a finding key to its issue URL."""
    run = report["run"]
    status = ICONS[run["status"]]
    started = run["started_at"][:16].replace("T", " ")
    lines = [f"## {status} daily · {started} UTC · {duration(run['seconds'])}", ""]
    if run["failed_step"]:
        lines += [f"**Failed at: {run['failed_step']}**", ""]

    lines += ["| Step | Result | Time |", "|---|---|---|"]
    for step in report["steps"]:
        icon = ICONS.get(step["outcome"], step["outcome"])
        lines.append(f"| {step['name']} | {icon} | {duration(step['seconds'])} |")

    if report["landing"] is not None:
        lines += ["", "### Bronze", "", "| Series | Chunks checked | New | Local days |"]
        lines.append("|---|---|---|---|")
        for entry in report["landing"]:
            days = " → ".join(entry["dates"]) if entry["dates"] else "-"
            row = f"{entry['series']} {entry['resolution']}"
            lines.append(f"| {row} | {entry['checked']} | {entry['written']} | {days} |")

    if report["silver"] is not None:
        lines += ["", "### Silver", ""]
        if report["silver"]:
            lines += ["| Series | New rows | Changed values |", "|---|---|---|"]
            lines += [
                f"| {s['series']} | {s['inserted']} | {s['revised']} |" for s in report["silver"]
            ]
        else:
            lines.append("No new or changed values.")

    if report["dbt"] is not None:
        dbt = report["dbt"]
        lines += ["", "### Gold", "", f"dbt: {dbt['passed']} passed, {dbt['failed']} failed, "
                  f"{dbt['warned']} warned, {dbt['skipped']} skipped"]  # fmt: skip
        if dbt["failed_nodes"]:
            lines.append(f"Failed: {', '.join(dbt['failed_nodes'])}")

    if report["export"] is not None:
        lines += ["", "### Export", "", "| Table | Rows | Up to |", "|---|---|---|"]
        for name, table in report["export"]["tables"].items():
            lines.append(f"| {name} | {table['rows']:,} | {table['max_time']} |")

    if report["api"] is not None:
        api = report["api"]
        state = (
            f"✅ {api['ms']} ms, data exported {api['age_minutes']} min ago"
            if api["ok"]
            else f"❌ {api.get('status') or api.get('error')}"
        )
        lines += ["", "### API", "", state]

    lines += ["", "### Irregularities", ""]
    if not report["findings"]:
        lines.append("None.")
    for finding in report["findings"]:
        link = (issues or {}).get(finding["key"])
        title = f"[{finding['title']}]({link})" if link else finding["title"]
        days = ", ".join(f"{day} {detail}" for day, detail in finding["days"].items())
        lines.append(f"- ⚠️ {title}: {days}")
    return "\n".join(lines) + "\n"


def collect_warehouse(
    http_path: str, started_at: datetime, now: datetime, profile: str | None = None
) -> tuple[list, list]:
    """Silver changes in this run and recent coverage; raises if the warehouse is unreachable."""
    from spotgrid_lakehouse.export.gold import SqlWarehouse

    warehouse = SqlWarehouse(http_path, profile)
    try:
        changes = warehouse.query(SILVER_CHANGES_SQL, {"started_at": started_at}).to_pylist()
        first_day: date = now.date() - timedelta(days=WINDOW_DAYS)
        coverage = warehouse.query(COVERAGE_SQL, {"first_day": first_day}).to_pylist()
    finally:
        warehouse.close()
    return changes, coverage
