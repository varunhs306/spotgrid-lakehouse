"""Short Telegram messages: a digest of every run and a note when an issue opens or closes."""

from __future__ import annotations

from datetime import datetime
from html import escape

import httpx

from spotgrid_lakehouse.monitoring.issues import Change
from spotgrid_lakehouse.monitoring.report import duration


def _link(url: str, text: str) -> str:
    return f'<a href="{escape(url)}">{escape(text)}</a>'


def digest(report: dict, open_issues: int, incident_url: str | None = None) -> str:
    run = report["run"]
    started = datetime.fromisoformat(run["started_at"]).strftime("%d %b")
    if run["status"] == "failure":
        lines = [f"❌ <b>daily failed</b> at {escape(run['failed_step'])} · {started}"]
        links = [_link(incident_url, "incident")] if incident_url else []
        lines.append(" · ".join([*links, _link(run["url"], "run log")]))
        return "\n".join(lines)

    lines = [f"✅ <b>daily</b> {started} · {duration(run['seconds'])}"]
    facts = []
    if report["export"]:
        latest = report["export"]["tables"]["hourly_features"]["max_time"]
        facts.append(f"data to {datetime.fromisoformat(latest).strftime('%d %b %H:%M')} UTC")
    if report["silver"] is not None:
        added = sum(s["inserted"] for s in report["silver"])
        changed = sum(s["revised"] for s in report["silver"])
        facts.append(f"+{added:,} rows · {changed:,} changed")
    if facts:
        lines.append(" · ".join(facts))
    api = report["api"]
    state = "API ok" if api and api["ok"] else "API not checked"
    lines.append(f"{state} · {open_issues} open issue{'s' if open_issues != 1 else ''}")
    lines.append(_link(run["url"], "run summary"))
    return "\n".join(lines)


def change_message(change: Change) -> str:
    icon, verb = ("⚠️", "") if change.action == "opened" else ("✅", "resolved: ")
    lines = [f"{icon} {verb}<b>{escape(change.title)}</b>"]
    if change.detail:
        lines.append(escape(change.detail))
    lines.append(_link(change.url, "issue"))
    return "\n".join(lines)


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
