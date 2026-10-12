import json

import httpx
import pytest

from spotgrid_lakehouse.monitoring.issues import DATA_QUALITY, INCIDENT, Change
from spotgrid_lakehouse.monitoring.telegram import Telegram, change_message, digest

RUN = "https://github.com/o/r/actions/runs/1"
ISSUE = "https://github.com/o/r/issues/9"
STEPS = ("land bronze", "merge silver", "build gold", "export")
COMPLETE = {
    "Prices": {"to": "2026-10-13", "due": "2026-10-13", "problems": {}},
    "Actuals": {"to": "2026-10-11", "due": "2026-10-11", "problems": {}},
    "Forecasts": {"to": "2026-10-12", "due": "2026-10-12", "problems": {}},
}
TOMORROW = {
    "local_date": "2026-10-13",
    "avg_price_eur_mwh": 197.23,
    "min_price_eur_mwh": 15.4,
    "max_price_eur_mwh": 234.79,
    "negative_price_hours": 0,
}


def report(status="success", failed_step=None, outcomes=None, findings=(), complete=COMPLETE):
    outcomes = outcomes or dict.fromkeys(STEPS, "success")
    return {
        "run": {
            "url": RUN,
            "started_at": "2026-10-12T15:17:00+00:00",
            "seconds": 202,
            "status": status,
            "failed_step": failed_step,
        },
        "steps": [{"name": s, "outcome": outcomes[s], "seconds": None} for s in STEPS],
        "silver": [
            {"series": "LOAD", "inserted": 21, "revised": 40},
            {"series": "PRICE_DE_LU", "inserted": 180, "revised": 14},
        ],
        "dbt": {"passed": 11, "failed": 0, "warned": 0, "skipped": 0, "failed_nodes": []},
        "api": {"ok": True, "ms": 218, "age_minutes": 0, "tomorrow": TOMORROW},
        "complete": complete,
        "findings": list(findings),
    }


def test_digest_of_a_good_run():
    assert digest(report(), issues=[]) == "\n".join(
        [
            "✅ <b>spotgrid daily</b>",
            "Time: Mon 12 Oct, 17:17",
            "Duration: 3m22s",
            "Pipeline: ✅ all steps passed",
            "New rows: 201",
            "Changed values: 54",
            "dbt: 11/11 passed",
            "",
            "📊 <b>Complete up to</b>",
            "Prices: Tue 13 Oct ✅",
            "Actuals: Sun 11 Oct ✅",
            "Forecasts: Mon 12 Oct ✅",
            "",
            "💶 <b>Tomorrow (13 Oct)</b>",
            "Average: 197 €/MWh",
            "Min: 15 €/MWh",
            "Max: 235 €/MWh",
            "Negative hours: 0",
            "",
            "🌐 <b>API</b>",
            "Status: ok (218 ms)",
            "Data age: 0 min",
            "Open issues: 🟢 none",
            "",
            f'<a href="{RUN}">Run summary</a>',
        ]
    )


def test_digest_with_a_data_problem():
    complete = COMPLETE | {
        "Prices": {"to": "2026-10-09", "due": "2026-10-13", "problems": {"2026-10-10": "0/24 h"}}
    }
    finding = {"key": "missing:PRICE_DE_LU", "title": "t", "days": {"2026-10-10": "0/24 h"}}

    text = digest(report(findings=[finding], complete=complete), issues=[(9, ISSUE)])

    assert text.startswith("⚠️ <b>spotgrid daily</b>")
    assert "Prices: Fri 09 Oct ❌ (10 Oct: 0/24 h)" in text
    assert f'Open issues: 🟠 1 (<a href="{ISSUE}">#9</a>)' in text


def test_long_problem_lists_are_cut():
    problems = {f"2026-10-0{d}": "0/24 h" for d in range(1, 6)}
    complete = COMPLETE | {
        "Prices": {"to": "2026-09-30", "due": "2026-10-13", "problems": problems}
    }

    text = digest(report(complete=complete), issues=[])

    assert "(01 Oct: 0/24 h, 02 Oct: 0/24 h, 03 Oct: 0/24 h, +2 more)" in text


def test_digest_of_a_failed_run():
    outcomes = {
        "land bronze": "success",
        "merge silver": "failure",
        "build gold": "skipped",
        "export": "skipped",
    }
    failed = report(status="failure", failed_step="merge silver", outcomes=outcomes)
    failed["api"]["age_minutes"] = 14 * 60

    text = digest(failed, issues=[], incident=(10, "https://github.com/o/r/issues/10"))

    assert text == "\n".join(
        [
            "❌ <b>spotgrid daily FAILED</b>",
            "Time: Mon 12 Oct, 17:17",
            "Failed step: merge silver",
            "Bronze: ✅",
            "Gold: ⏭ skipped",
            "Export: ⏭ skipped",
            "API: serving last good data (14 h old)",
            'Incident: 🔴 <a href="https://github.com/o/r/issues/10">#10</a>',
            "",
            f'<a href="{RUN}">Run log</a>',
        ]
    )


def test_issue_notes():
    opened = Change(
        "opened", "Missing hours: PRICE_DE_LU", ISSUE, 9, DATA_QUALITY, {"2026-10-10": "0/24 h"}
    )
    closed = Change(
        "closed",
        "Missing hours: PRICE_DE_LU",
        ISSUE,
        9,
        DATA_QUALITY,
        {"2026-10-10": "after 2d 4h"},
    )
    recovered = Change(
        "closed", "Daily pipeline failed at export", ISSUE, 10, INCIDENT, detail="after 1d 2h"
    )

    assert change_message(opened) == (
        f'🟠 <b>New issue</b> <a href="{ISSUE}">#9</a>\n'
        "Problem: Missing hours: PRICE_DE_LU\n"
        "Days: 10 Oct (0/24 h)"
    )
    assert change_message(closed).endswith("Days: 10 Oct filled after 2d 4h")
    assert change_message(recovered).endswith("Recovered: after 1d 2h")
    assert change_message(Change("opened", "t", ISSUE, 10, INCIDENT)) is None


def test_send_posts_html_to_the_chat():
    sent = []

    def handler(request):
        sent.append((request.url.path, json.loads(request.content)))
        return httpx.Response(200, json={"ok": True})

    bot = Telegram("123:secret", "42", httpx.Client(transport=httpx.MockTransport(handler)))
    bot.send("<b>hi</b>")

    assert sent[0][0] == "/bot123:secret/sendMessage"
    assert sent[0][1]["chat_id"] == "42"
    assert sent[0][1]["parse_mode"] == "HTML"


def test_errors_never_carry_the_token():
    rejecting = httpx.MockTransport(lambda request: httpx.Response(401))
    bot = Telegram("123:secret", "42", httpx.Client(transport=rejecting))

    with pytest.raises(RuntimeError) as error:
        bot.send("hi")

    assert "secret" not in str(error.value)
    assert error.value.__cause__ is None
