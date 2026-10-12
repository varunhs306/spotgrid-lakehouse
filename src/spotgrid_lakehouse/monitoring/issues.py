"""GitHub issues as the record of incidents and data problems.

One issue per problem, found again on later runs by a key hidden in its body. Data-quality
issues also keep, per local day, when the problem was first seen, so closing one says how long
it lasted. The issues are public and timestamped: the pipeline's on-call history.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

import httpx

INCIDENT = "incident"
DATA_QUALITY = "data-quality"
LABELS = {
    INCIDENT: ("d73a4a", "The daily pipeline failed"),
    DATA_QUALITY: ("fbca04", "Gaps or changes in the published data"),
}
INCIDENT_KEY = "incident:daily"
KEY = re.compile(r"<!-- spotgrid:key=(.+?) -->")
STATE = re.compile(r"<!-- spotgrid:state=(.+?) -->")


class Issues(Protocol):
    def open_issues(self, label: str) -> list[dict]: ...

    def create(self, title: str, body: str, label: str) -> dict: ...

    def comment(self, number: int, body: str) -> None: ...

    def edit(self, number: int, body: str) -> None: ...

    def close(self, number: int, comment: str) -> None: ...


@dataclass(frozen=True)
class Change:
    action: str  # opened or closed
    title: str
    url: str
    number: int
    label: str
    # Local day -> the problem when opened, or how long it lasted when closed.
    days: dict[str, str] = field(default_factory=dict)
    detail: str = ""


def since(start: datetime, now: datetime) -> str:
    minutes = int((now - start).total_seconds() // 60)
    days, minutes = divmod(minutes, 24 * 60)
    hours, minutes = divmod(minutes, 60)
    return f"{days}d {hours}h" if days else f"{hours}h {minutes}m"


def stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M UTC")


def key_of(issue: dict) -> str | None:
    match = KEY.search(issue.get("body") or "")
    return match.group(1) if match else None


def state_of(issue: dict) -> dict:
    match = STATE.search(issue.get("body") or "")
    return json.loads(match.group(1)) if match else {}


def finding_body(finding: dict, state: dict, run_url: str) -> str:
    rows = [
        f"| {day} | {entry['detail']} | {stamp(datetime.fromisoformat(entry['since']))} |"
        for day, entry in sorted(state.items())
    ]
    return "\n".join(
        [
            f"<!-- spotgrid:key={finding['key']} -->",
            f"<!-- spotgrid:state={json.dumps(state, sort_keys=True)} -->",
            f"{finding['title']}, found by the daily run. Local dates, Europe/Berlin.",
            "",
            "| Local day | Problem | First seen |",
            "|---|---|---|",
            *rows,
            "",
            f"Latest run: {run_url}. This issue closes itself once the problem is gone.",
        ]
    )


def sync_findings(
    issues: Issues, findings: list[dict], run_url: str, now: datetime
) -> tuple[list[Change], dict[str, str]]:
    """Open an issue per new finding, update changed ones, close the resolved ones.

    Returns the changes and the URL of every open data-quality issue by finding key.
    """
    existing = {key_of(i): i for i in issues.open_issues(DATA_QUALITY) if key_of(i)}
    changes, links = [], {}
    for finding in findings:
        issue = existing.pop(finding["key"], None)
        old = state_of(issue) if issue else {}
        state = {
            day: {"detail": detail, "since": old.get(day, {}).get("since", now.isoformat())}
            for day, detail in finding["days"].items()
        }
        if issue is None:
            issue = issues.create(
                finding["title"], finding_body(finding, state, run_url), DATA_QUALITY
            )
            problems = {day: entry["detail"] for day, entry in state.items()}
            opened = Change(
                "opened",
                finding["title"],
                issue["html_url"],
                issue["number"],
                DATA_QUALITY,
                problems,
            )
            changes.append(opened)
        elif state != old:
            notes = []
            if added := sorted(state.keys() - old.keys()):
                notes.append(f"New: {', '.join(f'{d} ({state[d]["detail"]})' for d in added)}")
            if gone := resolved(old, state.keys(), now):
                notes.append(f"Resolved: {listing(gone)}")
            if notes:
                issues.comment(issue["number"], "\n\n".join(notes) + f"\n\nRun: {run_url}")
            issues.edit(issue["number"], finding_body(finding, state, run_url))
        links[finding["key"]] = issue["html_url"]

    for issue in existing.values():
        gone = resolved(state_of(issue), set(), now)
        issues.close(issue["number"], f"Resolved: {listing(gone)}\n\nRun: {run_url}")
        closed = Change(
            "closed", issue["title"], issue["html_url"], issue["number"], DATA_QUALITY, gone
        )
        changes.append(closed)
    return changes, links


def sync_incident(
    issues: Issues, failed_step: str | None, run_url: str, now: datetime
) -> list[Change]:
    """One open incident while the pipeline is failing; closed by the next good run."""
    current = next((i for i in issues.open_issues(INCIDENT) if key_of(i) == INCIDENT_KEY), None)
    if failed_step is None:
        if current is None:
            return []
        down = since(datetime.fromisoformat(current["created_at"]), now)
        issues.close(current["number"], f"Recovered after {down}.\n\nRun: {run_url}")
        recovered = Change(
            "closed",
            current["title"],
            current["html_url"],
            current["number"],
            INCIDENT,
            detail=f"after {down}",
        )
        return [recovered]
    if current is not None:
        issues.comment(current["number"], f"Failed again at **{failed_step}**.\n\nRun: {run_url}")
        return []
    title = f"Daily pipeline failed at {failed_step}"
    body = "\n".join(
        [
            f"<!-- spotgrid:key={INCIDENT_KEY} -->",
            f"The daily run failed at **{failed_step}** at {stamp(now)}.",
            "",
            f"Run: {run_url}",
            "",
            "Each step is safe to rerun; see docs/runbooks/daily-pipeline.md. This issue closes "
            "itself after the next successful run.",
        ]
    )
    issue = issues.create(title, body, INCIDENT)
    return [Change("opened", title, issue["html_url"], issue["number"], INCIDENT)]


def resolved(old: dict, still_open: set[str], now: datetime) -> dict[str, str]:
    """Days no longer affected, with how long each one was."""
    return {
        day: f"after {since(datetime.fromisoformat(entry['since']), now)}"
        for day, entry in sorted(old.items())
        if day not in still_open
    }


def listing(days: dict[str, str]) -> str:
    return ", ".join(f"{day} {text}" for day, text in days.items())


class GitHubIssues:
    """The Issues REST API for one repository, with the workflow's GITHUB_TOKEN."""

    def __init__(self, repo: str, token: str, http: httpx.Client | None = None) -> None:
        self._http = http or httpx.Client(
            base_url=f"https://api.github.com/repos/{repo}",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=30,
        )

    def ensure_labels(self) -> None:
        for name, (color, description) in LABELS.items():
            response = self._http.post(
                "/labels", json={"name": name, "color": color, "description": description}
            )
            if response.status_code != 422:  # 422: the label exists
                response.raise_for_status()

    def open_issues(self, label: str) -> list[dict]:
        response = self._http.get(
            "/issues", params={"state": "open", "labels": label, "per_page": 100}
        )
        response.raise_for_status()
        return [i for i in response.json() if "pull_request" not in i]

    def create(self, title: str, body: str, label: str) -> dict:
        response = self._http.post(
            "/issues", json={"title": title, "body": body, "labels": [label]}
        )
        response.raise_for_status()
        return response.json()

    def comment(self, number: int, body: str) -> None:
        self._http.post(f"/issues/{number}/comments", json={"body": body}).raise_for_status()

    def edit(self, number: int, body: str) -> None:
        self._http.patch(f"/issues/{number}", json={"body": body}).raise_for_status()

    def close(self, number: int, comment: str) -> None:
        self.comment(number, comment)
        self._http.patch(
            f"/issues/{number}", json={"state": "closed", "state_reason": "completed"}
        ).raise_for_status()
