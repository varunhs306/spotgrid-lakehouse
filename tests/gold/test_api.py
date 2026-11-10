"""The v2 API over an export of the synthetic warehouse."""

import csv
import io
from datetime import UTC, datetime

import duckdb
import pytest
from fastapi.testclient import TestClient

from spotgrid_lakehouse.api import app as api_app
from spotgrid_lakehouse.api.app import create_app
from spotgrid_lakehouse.api.data import GoldData
from spotgrid_lakehouse.export.gold import ATTRIBUTION, LocalStore, export

EXPORTED = datetime(2026, 10, 26, 7, tzinfo=UTC)


@pytest.fixture(scope="module")
def store(built, tmp_path_factory):
    con = duckdb.connect(built.path, read_only=True)
    con.execute("SET TimeZone = 'UTC'")
    store = LocalStore(tmp_path_factory.mktemp("serving"))
    export(con, store, now=lambda: EXPORTED)
    con.close()
    return store


@pytest.fixture
def client(store):
    app = create_app(
        GoldData(store),
        now=lambda: datetime(2026, 10, 26, 9, tzinfo=UTC),
        link=lambda key: f"https://example.test/{key}",
    )
    return TestClient(app)


def get(client, path, **params):
    response = client.get(path, params=params, headers={"Accept-Encoding": "identity"})
    assert response.status_code == 200, response.text
    return response


def test_grid_defaults_to_germany_hourly_over_the_newest_week(client):
    body = get(client, "/v2/grid").json()

    assert body["attribution"] == ATTRIBUTION
    assert (body["resolution"], body["start"], body["end"]) == ("hour", "2026-10-19", "2026-10-25")
    assert len(body["rows"]) == 49
    first = body["rows"][0]
    assert (first["region"], first["ts_utc"], first["local_date"]) == (
        "DE", "2026-10-23T22:00:00Z", "2026-10-24",
    )  # fmt: skip
    assert first["wind_onshore_forecast_error_mwh"] == 50.0
    assert "updated_at" not in first


def test_one_endpoint_serves_every_resolution(client):
    counts = {
        res: len(get(client, "/v2/grid", resolution=res).json()["rows"])
        for res in ("15min", "hour", "day", "month")
    }
    assert counts == {"15min": 49 * 4, "hour": 49, "day": 2, "month": 1}


def test_several_regions_and_chosen_fields(client):
    rows = get(
        client, "/v2/grid", region="DE,AT", fields="load_mwh,wind_offshore_mwh", resolution="day"
    ).json()["rows"]

    assert {r["region"] for r in rows} == {"DE", "AT"}
    assert set(rows[0]) == {"region", "local_date", "load_mwh", "wind_offshore_mwh"}
    assert next(r for r in rows if r["region"] == "AT")["wind_offshore_mwh"] is None


def test_dates_narrow_the_rows(client):
    rows = get(client, "/v2/grid", start="2026-10-25", end="2026-10-25").json()["rows"]
    assert len(rows) == 25  # fall-back day


def test_prices_per_zone_with_daily_stats(client):
    rows = get(client, "/v2/prices", zone="DE-LU,FR", resolution="day").json()["rows"]

    assert [(r["zone"], r["local_date"]) for r in rows] == [
        ("DE-LU", "2026-10-24"), ("DE-LU", "2026-10-25"),
        ("FR", "2026-10-24"), ("FR", "2026-10-25"),
    ]  # fmt: skip
    assert {"base_eur_mwh", "peak_eur_mwh", "off_peak_eur_mwh", "negative_hours"} <= set(rows[0])


def test_csv(client):
    response = get(client, "/v2/prices", zone="FR", resolution="15min", format="csv")

    assert response.headers["content-type"].startswith("text/csv")
    assert response.headers["x-attribution"] == ATTRIBUTION
    rows = list(csv.DictReader(io.StringIO(response.text)))
    assert len(rows) == 49 * 4
    assert rows[0]["ts_utc"] == "2026-10-23T22:00:00Z"


def test_gzip_when_accepted(client):
    response = client.get("/v2/grid", headers={"Accept-Encoding": "gzip"})

    assert response.headers["content-encoding"] == "gzip"
    assert len(response.json()["rows"]) == 49


def test_too_many_values_point_to_bulk(client, monkeypatch):
    monkeypatch.setattr(api_app, "MAX_CELLS", 100)

    response = client.get("/v2/grid")

    assert response.status_code == 413
    assert "/v2/bulk" in response.json()["detail"]


def test_bad_parameters_are_explained(client):
    assert client.get("/v2/grid", params={"region": "XX"}).status_code == 422
    assert client.get("/v2/prices", params={"zone": "PL"}).status_code == 422
    assert client.get("/v2/grid", params={"fields": "nope"}).status_code == 422
    assert client.get("/v2/grid", params={"resolution": "week"}).status_code == 422
    response = client.get("/v2/grid", params={"start": "2026-10-25", "end": "2026-10-24"})
    assert response.status_code == 422


def test_catalog(client):
    body = get(client, "/v2/catalog").json()

    assert body["exported_at"] == EXPORTED.isoformat()
    assert body["age_minutes"] == 120
    assert "DE-AT-LU" not in body["zones"] and "DE-LU" in body["zones"]
    nuclear = next(s for s in body["series"] if s["column"] == "nuclear_mwh")
    assert nuclear["ended"] == "2023-04-16"
    assert any(c["series"] == "price_eur_mwh" and c["area"] == "FR" for c in body["coverage"])
    assert body["tables"]["grid_hourly"]["rows"] == 49 * 9


def test_bulk_lists_monthly_files(client):
    body = get(client, "/v2/bulk", dataset="prices", resolution="15min", area="FR").json()

    assert [f["month"] for f in body["files"]] == ["2026-10"]
    assert body["files"][0]["url"] == "https://example.test/bulk/prices_quarterhour/FR/2026-10.csv"
    assert client.get("/v2/bulk", params={"resolution": "day"}).status_code == 422


def test_revisions_empty_when_none_logged(client):
    body = get(client, "/v2/revisions", start="2026-10-01", end="2026-10-31").json()
    assert body["rows"] == []


def test_responses_are_cacheable(client):
    response = get(client, "/v2/grid")
    assert response.headers["cache-control"] == "public, max-age=300"
