"""Report on a pipeline run and raise its alerts. Runs as the last step of every run.

python -m spotgrid_lakehouse.monitoring --started-at 2026-10-12T15:17:00Z --dry-run
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, date, datetime
from pathlib import Path

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
    parser.add_argument(
        "--seconds", action="append", default=[], help="step=seconds, e.g. 'export=12'"
    )
    parser.add_argument("--warehouse", help="query this warehouse file for silver checks")
    parser.add_argument("--summary", type=Path, help="also write the summary to this file")
    parser.add_argument("--dry-run", action="store_true", help="print, alert nobody")
    args = parser.parse_args(argv)

    now = datetime.now(UTC)
    # Run by hand without outcomes, every step is taken to have succeeded.
    outcomes = dict(item.split("=", 1) for item in args.outcome) or dict.fromkeys(STEPS, "success")
    seconds = {k: int(v) for k, v in (item.split("=", 1) for item in args.seconds)}
    repo = env.get("GITHUB_REPOSITORY")
    run_url = env.get("RUN_URL", "(local run)")

    silver_changes = coverage = quarantine = None
    if args.warehouse:
        try:
            silver_changes, coverage, quarantine = collect_warehouse(
                args.warehouse, args.started_at, now
            )
        except Exception as exc:  # the report must still go out
            print(f"warning: warehouse checks skipped: {type(exc).__name__}: {exc}")

    report = build(
        run={"url": run_url, "trigger": env.get("TRIGGER", "manual")},
        started_at=args.started_at,
        now=now,
        outcomes=outcomes,
        seconds=seconds,
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
        # A skipped run proves no processing recovery. An export whose provenance write
        # failed is not a verified recovery either (the failure is a named report step).
        if report["run"]["failed_step"] or outcomes.get("export") == "success":
            changes += sync_incident(github, report["run"]["failed_step"], run_url, now)
        if report["run"]["status"] == "success" and coverage is not None:
            found, links = sync_findings(github, report["findings"], run_url, now)
            changes += found
        else:  # leave data issues as they are; only link the open ones
            links = {key_of(i): i["html_url"] for i in github.open_issues(DATA_QUALITY)}
        incident = next(((i["number"], i["html_url"]) for i in github.open_issues(INCIDENT)), None)

    summary = markdown(report, links)
    print(summary)
    if args.summary:
        args.summary.write_text(summary, encoding="utf-8")

    if args.dry_run:
        print(digest(report, [], None))
    bot_token, chat_id = env.get("TELEGRAM_BOT_TOKEN"), env.get("TELEGRAM_CHAT_ID")
    if bot_token and chat_id and not args.dry_run:
        bot = Telegram(bot_token, chat_id)
        issues = sorted((int(url.rsplit("/", 1)[1]), url) for url in links.values())
        problem = (
            report["run"]["status"] == "failure"
            or report["findings"]
            or changes
            or report["source_freshness"]["status"] != "up_to_date"
            or any(row.get("source_gaps") for row in report["source_freshness"]["series"])
        )
        try:
            quiet_poll = env.get("QUIET_POLL") == "true"
            if (
                report["run"]["status"] == "failure"
                or changes
                or (not quiet_poll and (problem or not quiet(report["run"]["trigger"], now)))
            ):
                bot.send(digest(report, issues, incident))
            for change in changes:
                if message := change_message(change):
                    bot.send(message)
        except RuntimeError as exc:
            print(f"warning: {exc}")


if __name__ == "__main__":
    main()
