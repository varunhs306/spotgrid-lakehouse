"""Telegram messages: a digest of the run and a note when an issue opens or closes.

One fact per line as "Name: value", in Berlin time, readable on a phone lock screen.
"""

from __future__ import annotations

from datetime import date, datetime
from html import escape

import httpx

from spotgrid_lakehouse.monitoring.checks import BERLIN
from spotgrid_lakehouse.monitoring.issues import INCIDENT, Change
from spotgrid_lakehouse.monitoring.report import duration

STEP_LABELS = {
    "land bronze": "Bronze",
    "merge silver": "Silver",
    "build gold": "Gold",
    "export": "Export",
}
OUTCOMES = {"success": "✅", "failure": "❌", "cancelled": "⛔ cancelled", "skipped": "⏭ skipped"}
# Problems listed per group before the rest is summed up.
MAX_PROBLEMS = 3


def link(url: str, text: str) -> str:
    return f'<a href="{escape(url)}">{escape(text)}</a>'


def day(iso: str, weekday: bool = False) -> str:
    return date.fromisoformat(iso).strftime("%a %d %b" if weekday else "%d %b")


def age(minutes: int | None) -> str:
    if minutes is None:
        return "unknown"
    return f"{minutes // 60} h" if minutes >= 120 else f"{minutes} min"


def issue_list(issues: list[tuple[int, str]]) -> str:
    if not issues:
        return "🟢 none"
    return f"🟠 {len(issues)} (" + ", ".join(link(url, f"#{n}") for n, url in issues) + ")"


def complete_line(group: str, entry: dict) -> str:
    if entry["to"] is None:
        return f"{group}: ❌ no complete day"
    line = f"{group}: {day(entry['to'], weekday=True)}"
    if not entry["problems"]:
        return line + (" ✅" if entry["to"] >= entry["due"] else " ❌")
    problems = [f"{day(d)}: {escape(p)}" for d, p in sorted(entry["problems"].items())]
    if len(problems) > MAX_PROBLEMS:
        problems = [*problems[:MAX_PROBLEMS], f"+{len(problems) - MAX_PROBLEMS} more"]
    return f"{line} ❌ ({', '.join(problems)})"


def digest(
    report: dict,
    issues: list[tuple[int, str]],
    incident: tuple[int, str] | None = None,
) -> str:
    """`issues` are the open data-quality issues as (number, url); `incident` the open one."""
    run = report["run"]
    when = datetime.fromisoformat(run["started_at"]).astimezone(BERLIN).strftime("%a %d %b, %H:%M")

    if run["status"] == "failure":
        lines = ["❌ <b>spotgrid daily FAILED</b>", f"Time: {when}"]
        lines.append(f"Failed step: {escape(run['failed_step'])}")
        for step in report["steps"]:
            if step["name"] != run["failed_step"]:
                outcome = OUTCOMES.get(step["outcome"], step["outcome"])
                lines.append(f"{STEP_LABELS[step['name']]}: {outcome}")
        api = report["api"]
        if api and api["ok"]:
            lines.append(f"API: serving last good data ({age(api['age_minutes'])} old)")
        elif api:
            lines.append(f"API: ❌ down ({api.get('status') or api.get('error')})")
        if incident:
            lines.append(f"Incident: 🔴 {link(incident[1], f'#{incident[0]}')}")
        return "\n".join([*lines, "", link(run["url"], "Run log")])

    healthy = not report["findings"] and not issues
    lines = [f"{'✅' if healthy else '⚠️'} <b>spotgrid daily</b>", f"Time: {when}"]
    lines += [f"Duration: {duration(run['seconds'])}", "Pipeline: ✅ all steps passed"]
    if report["silver"] is not None:
        lines.append(f"New rows: {sum(s['inserted'] for s in report['silver']):,}")
        lines.append(f"Changed values: {sum(s['revised'] for s in report['silver']):,}")
    if quality := report.get("quality"):
        lines.append(f"Quarantined: {quality['quarantined']:,}")
    if dbt := report["dbt"]:
        total = dbt["passed"] + dbt["failed"] + dbt["warned"] + dbt["skipped"]
        lines.append(f"dbt: {dbt['passed']}/{total} passed")

    if report.get("complete"):
        lines += ["", "📊 <b>Complete up to</b>"]
        lines += [complete_line(group, entry) for group, entry in report["complete"].items()]

    api = report["api"] or {}
    if tomorrow := api.get("tomorrow"):
        lines += ["", f"💶 <b>Tomorrow ({day(tomorrow['local_date'])})</b>"]
        lines += [
            f"Average: {tomorrow['avg_price_eur_mwh']:.0f} €/MWh",
            f"Min: {tomorrow['min_price_eur_mwh']:.0f} €/MWh",
            f"Max: {tomorrow['max_price_eur_mwh']:.0f} €/MWh",
            f"Negative hours: {tomorrow['negative_price_hours']}",
        ]

    lines += ["", "🌐 <b>API</b>"]
    if api:
        lines.append(f"Status: ok ({api['ms']} ms)")
        lines.append(f"Data age: {age(api['age_minutes'])}")
    else:
        lines.append("Status: not checked")
    lines.append(f"Open issues: {issue_list(issues)}")
    return "\n".join([*lines, "", link(run["url"], "Run summary")])


def change_message(change: Change) -> str | None:
    """A note for an opened or resolved issue; a new incident is already in the digest."""
    number = link(change.url, f"#{change.number}")
    if change.action == "opened":
        if change.label == INCIDENT:
            return None
        days = ", ".join(f"{day(d)} ({escape(p)})" for d, p in sorted(change.days.items()))
        lines = [f"🟠 <b>New issue</b> {number}", f"Problem: {escape(change.title)}"]
        return "\n".join([*lines, f"Days: {days}"])
    lines = [f"🟢 <b>Resolved</b> {number}", f"Problem: {escape(change.title)}"]
    if change.label == INCIDENT:
        return "\n".join([*lines, f"Recovered: {escape(change.detail)}"])
    days = ", ".join(f"{day(d)} filled {escape(p)}" for d, p in sorted(change.days.items()))
    return "\n".join([*lines, f"Days: {days}"])


class Telegram:
    def __init__(self, token: str, chat_id: str, http: httpx.Client | None = None) -> None:
        self._url = f"https://api.telegram.org/bot{token}/sendMessage"
        self._chat_id = chat_id
        self._http = http or httpx.Client(timeout=30)

    def send(self, text: str) -> None:
        # The token is part of the URL, so errors must not carry the request.
        try:
            response = self._http.post(
                self._url,
                json={
                    "chat_id": self._chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
            )
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Telegram unreachable: {type(exc).__name__}") from None
        if response.status_code != 200:
            raise RuntimeError(f"Telegram rejected the message: HTTP {response.status_code}")
