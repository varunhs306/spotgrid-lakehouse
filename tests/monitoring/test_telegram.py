import json

import httpx
import pytest

from spotgrid_lakehouse.monitoring.issues import Change
from spotgrid_lakehouse.monitoring.telegram import Telegram, change_message, digest

RUN = "https://github.com/o/r/actions/runs/1"


def report(**run):
    return {
        "run": {
            "url": RUN,
            "started_at": "2026-10-12T15:17:00+00:00",
            "seconds": 242,
            "status": "success",
            "failed_step": None,
        }
        | run,
        "silver": [
            {"series": "PRICE_DE_LU", "inserted": 24, "revised": 0},
            {"series": "SOLAR", "inserted": 79, "revised": 60},
        ],
        "export": {"tables": {"hourly_features": {"max_time": "2026-10-13T21:00:00+00:00"}}},
        "api": {"ok": True},
    }


def test_digest_of_a_good_run():
    assert digest(report(), open_issues=1) == (
        "✅ <b>daily</b> 12 Oct · 4m02s\n"
        "data to 13 Oct 21:00 UTC · +103 rows · 60 changed\n"
        "API ok · 1 open issue\n"
        f'<a href="{RUN}">run summary</a>'
    )


def test_digest_of_a_failed_run():
    text = digest(
        report(status="failure", failed_step="merge silver"),
        open_issues=0,
        incident_url="https://github.com/o/r/issues/13",
    )

    assert text == (
        "❌ <b>daily failed</b> at merge silver · 12 Oct\n"
        '<a href="https://github.com/o/r/issues/13">incident</a> · '
        f'<a href="{RUN}">run log</a>'
    )


def test_change_messages():
    opened = Change("opened", "Missing hours: PRICE_DE_LU", "https://x/1", "2026-10-10 0/24 h")
    closed = Change("closed", "Missing hours: PRICE_DE_LU", "https://x/1", "2026-10-10 after 2d 4h")

    assert change_message(opened) == (
        '⚠️ <b>Missing hours: PRICE_DE_LU</b>\n2026-10-10 0/24 h\n<a href="https://x/1">issue</a>'
    )
    assert change_message(closed).startswith("✅ resolved: <b>Missing hours: PRICE_DE_LU</b>")


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
    bot = Telegram(
        "123:secret",
        "42",
        httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(401))),
    )

    with pytest.raises(RuntimeError) as error:
        bot.send("hi")

    assert "secret" not in str(error.value)
    assert error.value.__cause__ is None
