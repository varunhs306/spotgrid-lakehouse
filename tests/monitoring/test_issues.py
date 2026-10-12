from datetime import UTC, datetime, timedelta

import httpx

from spotgrid_lakehouse.monitoring.issues import (
    DATA_QUALITY,
    INCIDENT,
    GitHubIssues,
    sync_findings,
    sync_incident,
)

RUN = "https://github.com/o/r/actions/runs/1"
OCT_10 = datetime(2026, 10, 10, 17, 14, tzinfo=UTC)
GAP = {"key": "missing:PRICE_DE_LU", "title": "Missing hours: PRICE_DE_LU"}


class FakeIssues:
    def __init__(self, now=OCT_10):
        self.issues: dict[int, dict] = {}
        self.comments: dict[int, list[str]] = {}
        self.now = now

    def open_issues(self, label):
        return [i for i in self.issues.values() if i["state"] == "open" and label in i["labels"]]

    def create(self, title, body, label):
        number = len(self.issues) + 1
        self.issues[number] = {
            "number": number,
            "title": title,
            "body": body,
            "labels": [label],
            "state": "open",
            "created_at": self.now.isoformat(),
            "html_url": f"https://github.com/o/r/issues/{number}",
        }
        self.comments[number] = []
        return self.issues[number]

    def comment(self, number, body):
        self.comments[number].append(body)

    def edit(self, number, body):
        self.issues[number]["body"] = body

    def close(self, number, comment):
        self.comment(number, comment)
        self.issues[number]["state"] = "closed"


def test_a_gap_opens_one_issue_and_closes_when_filled():
    issues = FakeIssues()

    changes, links = sync_findings(issues, [GAP | {"days": {"2026-10-10": "0/24 h"}}], RUN, OCT_10)

    assert [(c.action, c.number, c.days) for c in changes] == [
        ("opened", 1, {"2026-10-10": "0/24 h"})
    ]
    assert links == {"missing:PRICE_DE_LU": "https://github.com/o/r/issues/1"}
    assert "| 2026-10-10 | 0/24 h | 2026-10-10 17:14 UTC |" in issues.issues[1]["body"]

    # Still missing a day later: nothing new to say, so no comment and no email.
    changes, _ = sync_findings(
        issues, [GAP | {"days": {"2026-10-10": "0/24 h"}}], RUN, OCT_10 + timedelta(days=1)
    )
    assert changes == []
    assert issues.comments[1] == []

    filled = OCT_10 + timedelta(days=2, hours=4)
    changes, links = sync_findings(issues, [], RUN, filled)

    assert [(c.action, c.days) for c in changes] == [("closed", {"2026-10-10": "after 2d 4h"})]
    assert issues.issues[1]["state"] == "closed"
    assert issues.comments[1] == [f"Resolved: 2026-10-10 after 2d 4h\n\nRun: {RUN}"]
    assert links == {}


def test_new_and_filled_days_are_commented_on_the_same_issue():
    issues = FakeIssues()
    sync_findings(issues, [GAP | {"days": {"2026-10-10": "0/24 h"}}], RUN, OCT_10)
    later = OCT_10 + timedelta(days=1)

    changes, _ = sync_findings(issues, [GAP | {"days": {"2026-10-11": "12/24 h"}}], RUN, later)

    assert changes == []
    assert len(issues.issues) == 1
    assert issues.comments[1] == [
        f"New: 2026-10-11 (12/24 h)\n\nResolved: 2026-10-10 after 1d 0h\n\nRun: {RUN}"
    ]
    assert "| 2026-10-11 | 12/24 h | 2026-10-11 17:14 UTC |" in issues.issues[1]["body"]
    assert "2026-10-10" not in issues.issues[1]["body"].split("-->")[-1]


def test_other_issues_are_left_alone():
    issues = FakeIssues()
    issues.create("Question about the API", "no key here", DATA_QUALITY)

    changes, _ = sync_findings(issues, [], RUN, OCT_10)

    assert changes == []
    assert issues.issues[1]["state"] == "open"


def test_incident_opens_once_and_closes_on_recovery():
    issues = FakeIssues()

    opened = sync_incident(issues, "merge silver", RUN, OCT_10)
    again = sync_incident(issues, "merge silver", RUN, OCT_10 + timedelta(days=1))
    closed = sync_incident(issues, None, RUN, OCT_10 + timedelta(days=1, hours=2))

    assert [(c.action, c.title) for c in opened] == [
        ("opened", "Daily pipeline failed at merge silver")
    ]
    assert again == []
    assert [(c.action, c.detail) for c in closed] == [("closed", "after 1d 2h")]
    assert issues.issues[1]["labels"] == [INCIDENT]
    assert issues.comments[1][0].startswith("Failed again at **merge silver**")
    assert issues.comments[1][1].startswith("Recovered after 1d 2h.")


def test_healthy_run_without_incident_does_nothing():
    assert sync_incident(FakeIssues(), None, RUN, OCT_10) == []


def test_rest_client_requests():
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path, request.content))
        if request.method == "GET":
            return httpx.Response(200, json=[{"number": 1}, {"number": 2, "pull_request": {}}])
        if request.url.path.endswith("/labels"):
            return httpx.Response(422, json={})
        return httpx.Response(201, json={"number": 3, "html_url": "u"})

    http = httpx.Client(
        base_url="https://api.github.com/repos/o/r", transport=httpx.MockTransport(handler)
    )
    client = GitHubIssues("o/r", "token", http)

    client.ensure_labels()
    assert client.open_issues(INCIDENT) == [{"number": 1}]
    client.close(5, "done")

    assert [(m, p) for m, p, _ in calls] == [
        ("POST", "/repos/o/r/labels"),
        ("POST", "/repos/o/r/labels"),
        ("GET", "/repos/o/r/issues"),
        ("POST", "/repos/o/r/issues/5/comments"),
        ("PATCH", "/repos/o/r/issues/5"),
    ]
