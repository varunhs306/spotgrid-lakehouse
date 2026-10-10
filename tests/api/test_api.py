from datetime import UTC, date, datetime, timedelta

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient
from mangum import Mangum

from spotgrid_lakehouse.api.app import create_app
from spotgrid_lakehouse.api.data import GoldData
from spotgrid_lakehouse.export.gold import ATTRIBUTION, MANIFEST_KEY, export

# Ten local days in October (CEST, UTC+2). The last day has prices and forecasts only, as the
# day-ahead auction publishes before the actuals.
FIRST_HOUR = datetime(2026, 9, 30, 22, tzinfo=UTC)
HOURS = [FIRST_HOUR + timedelta(hours=h) for h in range(10 * 24)]
LAST_DAY = date(2026, 10, 10)


def hourly() -> pa.Table:
    local_dates = [(ts + timedelta(hours=2)).date() for ts in HOURS]
    actual = [None if d == LAST_DAY else 1.0 for d in local_dates]
    columns = {
        "ts_utc": pa.array(HOURS, pa.timestamp("us", tz="UTC")),
        "local_date": pa.array(local_dates, pa.date32()),
        "price_eur_mwh": [float(h % 24) - 2 for h in range(len(HOURS))],
        "load_mwh": [None if a is None else 50_000.0 for a in actual],
        "wind_onshore_mwh": actual,
        "wind_offshore_mwh": actual,
        "solar_mwh": actual,
        "forecast_wind_onshore_mwh": [2.0] * len(HOURS),
        "forecast_wind_offshore_mwh": [2.0] * len(HOURS),
        "forecast_solar_mwh": [2.0] * len(HOURS),
        "residual_load_mwh": [None if a is None else 49_997.0 for a in actual],
        "source_resolution": ["hour"] * len(HOURS),
    }
    return pa.table(columns)


def daily() -> pa.Table:
    days = [date(2026, 10, d) for d in range(1, 11)]
    return pa.table(
        {
            "local_date": pa.array(days, pa.date32()),
            "min_price_eur_mwh": [-2.0] * 10,
            "avg_price_eur_mwh": [9.5] * 10,
            "max_price_eur_mwh": [21.0] * 10,
            "negative_price_hours": pa.array([2] * 10, pa.int64()),
            "price_hours": pa.array([24] * 10, pa.int64()),
        }
    )


class Warehouse:
    def __init__(self, tables):
        self.tables = tables

    def read(self, table, order_by):
        return self.tables[table]


class Bucket:
    """Both sides of the serving bucket: the export writes it, the API reads it."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.gets: list[str] = []

    def put(self, key, data, content_type):
        self.objects[key] = data

    def get(self, key):
        self.gets.append(key)
        return self.objects.get(key)


def exported_at():
    return datetime(2026, 10, 10, 5, 0, tzinfo=UTC)


@pytest.fixture
def bucket():
    bucket = Bucket()
    export(Warehouse({"hourly_features": hourly(), "daily_price_summary": daily()}), bucket)
    return bucket


@pytest.fixture
def client(bucket):
    return TestClient(create_app(GoldData(bucket)))


def local_dates(body):
    return sorted({row["local_date"] for row in body["rows"]})


def test_prices_default_to_the_latest_seven_days(client):
    response = client.get("/v1/prices")

    assert response.status_code == 200
    body = response.json()
    assert body["attribution"] == ATTRIBUTION
    assert (body["start"], body["end"]) == ("2026-10-04", "2026-10-10")
    assert len(body["rows"]) == 7 * 24
    assert body["rows"][0] == {
        "ts_utc": "2026-10-03T22:00:00Z",
        "local_date": "2026-10-04",
        "price_eur_mwh": -2.0,
    }
    assert response.headers["cache-control"] == "public, max-age=300"


def test_load_ends_on_the_latest_day_with_actuals(client):
    body = client.get("/v1/load").json()

    assert body["end"] == "2026-10-09"
    assert body["rows"][-1]["load_mwh"] == 50_000.0
    assert body["rows"][-1]["residual_load_mwh"] == 49_997.0


def test_generation_keeps_hours_that_only_have_forecasts(client):
    body = client.get("/v1/generation", params={"start": "2026-10-10"}).json()

    assert local_dates(body) == ["2026-10-10"]
    assert body["rows"][0]["solar_mwh"] is None
    assert body["rows"][0]["forecast_solar_mwh"] == 2.0


def test_explicit_window_is_inclusive(client):
    body = client.get("/v1/prices", params={"start": "2026-10-02", "end": "2026-10-03"}).json()

    assert local_dates(body) == ["2026-10-02", "2026-10-03"]
    assert len(body["rows"]) == 48


def test_one_bound_spans_seven_days(client):
    body = client.get("/v1/prices/daily", params={"end": "2026-10-08"}).json()

    assert (body["start"], body["end"]) == ("2026-10-02", "2026-10-08")
    assert [row["local_date"] for row in body["rows"]] == [f"2026-10-0{d}" for d in range(2, 9)]
    assert body["rows"][0]["negative_price_hours"] == 2


@pytest.mark.parametrize(
    "params",
    [
        {"start": "2026-10-05", "end": "2026-10-04"},
        {"start": "2025-01-01", "end": "2026-10-04"},
        {"start": "not-a-date"},
    ],
)
def test_bad_windows_are_rejected(client, params):
    assert client.get("/v1/prices", params=params).status_code == 422


def test_no_export_yet_is_unavailable():
    client = TestClient(create_app(GoldData(Bucket())))

    response = client.get("/v1/prices")

    assert response.status_code == 503
    assert "cache-control" not in response.headers


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def test_warm_reads_come_from_memory(bucket):
    clock = Clock()
    data = GoldData(bucket, ttl=300, clock=clock)

    data.table("hourly_features")
    clock.now = 299
    data.table("hourly_features")

    assert bucket.gets == [MANIFEST_KEY, "gold/hourly_features.parquet"]


def test_new_export_is_picked_up_after_the_ttl(bucket):
    clock = Clock()
    data = GoldData(bucket, ttl=300, clock=clock)
    data.table("daily_price_summary")
    newer = daily().slice(0, 3)
    export(Warehouse({"hourly_features": hourly(), "daily_price_summary": newer}), bucket)

    clock.now = 300
    table = data.table("daily_price_summary")

    assert table.num_rows == 3
    assert bucket.gets.count("gold/daily_price_summary.parquet") == 2


def test_unchanged_table_is_not_downloaded_again(bucket):
    clock = Clock()
    data = GoldData(bucket, ttl=300, clock=clock)
    data.table("hourly_features")
    export(Warehouse({"hourly_features": hourly(), "daily_price_summary": daily()}), bucket)

    clock.now = 300
    data.table("hourly_features")

    assert bucket.gets.count(MANIFEST_KEY) == 2
    assert bucket.gets.count("gold/hourly_features.parquet") == 1


def test_lambda_handler_serves_a_function_url_event(bucket):
    handler = Mangum(create_app(GoldData(bucket)), lifespan="off")
    event = {
        "version": "2.0",
        "routeKey": "$default",
        "rawPath": "/v1/prices/daily",
        "rawQueryString": "start=2026-10-09",
        "headers": {"host": "example.lambda-url.us-east-1.on.aws"},
        "requestContext": {
            "http": {"method": "GET", "path": "/v1/prices/daily", "sourceIp": "192.0.2.1"},
            "stage": "$default",
        },
        "isBase64Encoded": False,
    }

    response = handler(event, None)

    assert response["statusCode"] == 200
    assert '"local_date":"2026-10-10"' in response["body"]
