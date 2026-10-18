"""Report on a daily run and raise its alerts. Runs as the last step of the daily workflow.

python -m spotgrid_lakehouse.monitoring --started-at 2026-10-12T15:17:00Z --dry-run
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, date, datetime
from pathlib import Path

import httpx

from spotgrid_lakehouse.monitoring.checks import BERLIN, horizon
from spotgrid_lakehouse.monitoring.issues import (
    DATA_QUALITY,
    INCIDENT,
    GitHubIssues,
    key_of,
    sync_findings,
    sync_incident,
)
from spotgrid_lakehouse.monitoring.report import (
    STEPS,
    build,
    check_api,
    collect_warehouse,
    dbt_summary,
    markdown,
    read_json,
)
from spotgrid_lakehouse.monitoring.telegram import Telegram, change_message, digest
from spotgrid_lakehouse.sources.smard import Series


def step_seconds(env: dict) -> dict[str, int]:
    """Durations of this job's finished steps, from the Actions API. Best effort."""
    repo, run_id, token = (
        env.get(k) for k in ("GITHUB_REPOSITORY", "GITHUB_RUN_ID", "GITHUB_TOKEN")
    )
    if not (repo and run_id and token):
        return {}
    attempt = env.get("GITHUB_RUN_ATTEMPT", "1")
    try:
        response = httpx.get(
            f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/attempts/{attempt}/jobs",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=30,
        )
        response.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"warning: step times unavailable: {type(exc).__name__}")
        return {}
    seconds = {}
    for job in response.json()["jobs"]:
        for step in job["steps"]:
            if step.get("started_at") and step.get("completed_at"):
                start = datetime.fromisoformat(step["started_at"])
                end = datetime.fromisoformat(step["completed_at"])
                seconds[step["name"]] = int((end - start).total_seconds())
    return seconds


def tomorrow(now: datetime) -> date | None:
    """Tomorrow's local date once its prices are due, else None."""
    due = horizon(Series.PRICE_DE_LU, now)
    return due if due > now.astimezone(BERLIN).date() else None


def quiet(trigger: str, now: datetime) -> bool:
    """The scheduled morning run only messages when something is wrong."""
    return trigger == "schedule" and now.astimezone(BERLIN).hour < 12


def main(argv: list[str] | None = None, env: dict | None = None) -> None:
    env = dict(os.environ) if env is None else env
    parser = argparse.ArgumentParser(description="Report on a daily run and raise its alerts.")
    parser.add_argument("--started-at", required=True, type=datetime.fromisoformat)
    parser.add_argument("--landing", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--dbt-results", type=Path)
    parser.add_argument(
        "--outcome", action="append", default=[], help="step=outcome, e.g. 'export=success'"
    )
    parser.add_argument("--profile", help="Databricks config profile; default auth chain if unset")
    parser.add_argument("--dry-run", action="store_true", help="print, alert nobody")
    args = parser.parse_args(argv)

    now = datetime.now(UTC)
    # Run by hand without outcomes, every step is taken to have succeeded.
    outcomes = dict(item.split("=", 1) for item in args.outcome) or dict.fromkeys(STEPS, "success")
    repo, run_id = env.get("GITHUB_REPOSITORY"), env.get("GITHUB_RUN_ID")
    server = env.get("GITHUB_SERVER_URL", "https://github.com")
    run_url = f"{server}/{repo}/actions/runs/{run_id}" if repo and run_id else "(local run)"

    silver_changes = coverage = quarantine = None
    if http_path := env.get("DATABRICKS_HTTP_PATH"):
        try:
            silver_changes, coverage, quarantine = collect_warehouse(
                http_path, args.started_at, now, args.profile
            )
        except Exception as exc:  # the report must still go out
            print(f"warning: warehouse checks skipped: {type(exc).__name__}: {exc}")

    report = build(
        run={"url": run_url, "trigger": env.get("GITHUB_EVENT_NAME", "manual")},
        started_at=args.started_at,
        now=now,
        outcomes=outcomes,
        seconds=step_seconds(env),
        landing=read_json(args.landing),
        silver_changes=silver_changes,
        coverage=coverage,
        quarantine=quarantine,
        dbt=dbt_summary(read_json(args.dbt_results)),
        manifest=read_json(args.manifest),
        api=check_api(env.get("API_URL"), tomorrow(now)),
    )

    changes, links, incident = [], {}, None
    token = env.get("GITHUB_TOKEN")
    if token and repo and not args.dry_run:
        github = GitHubIssues(repo, token)
        github.ensure_labels()
        changes += sync_incident(github, report["run"]["failed_step"], run_url, now)
        if report["run"]["status"] == "success" and coverage is not None:
            found, links = sync_findings(github, report["findings"], run_url, now)
            changes += found
        else:  # leave data issues as they are; only link the open ones
            links = {key_of(i): i["html_url"] for i in github.open_issues(DATA_QUALITY)}
        incident = next(((i["number"], i["html_url"]) for i in github.open_issues(INCIDENT)), None)

    summary = markdown(report, links)
    print(summary)
    if path := env.get("GITHUB_STEP_SUMMARY"):
        with open(path, "a", encoding="utf-8") as file:
            file.write(summary)

    if args.dry_run:
        print(digest(report, [], None))
    bot_token, chat_id = env.get("TELEGRAM_BOT_TOKEN"), env.get("TELEGRAM_CHAT_ID")
    if bot_token and chat_id and not args.dry_run:
        bot = Telegram(bot_token, chat_id)
        issues = sorted((int(url.rsplit("/", 1)[1]), url) for url in links.values())
        problem = report["run"]["status"] == "failure" or report["findings"] or changes
        try:
            if problem or not quiet(report["run"]["trigger"], now):
                bot.send(digest(report, issues, incident))
            for change in changes:
                if message := change_message(change):
                    bot.send(message)
        except RuntimeError as exc:
            print(f"warning: {exc}")


if __name__ == "__main__":
    main()
