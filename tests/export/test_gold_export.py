import hashlib
import io
import json
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from spotgrid_lakehouse.export.gold import MANIFEST_KEY, PARQUET, export

HOURLY = pa.table(
    {
        "ts_utc": pa.array(
            [datetime(2026, 10, 8, 22, tzinfo=UTC), datetime(2026, 10, 8, 23, tzinfo=UTC)],
            pa.timestamp("us", tz="UTC"),
        ),
        "price_eur_mwh": [88.5, -1.25],
    }
)
DAILY = pa.table({"local_date": [date(2026, 10, 9)], "negative_price_hours": [1]})


class FakeWarehouse:
    def __init__(self, tables):
        self.tables = tables
        self.reads = []

    def read(self, table, order_by):
        self.reads.append((table, order_by))
        return self.tables[table]


class FakeStore:
    def __init__(self):
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.writes: list[str] = []

    def put(self, key, data, content_type):
        self.objects[key] = (data, content_type)
        self.writes.append(key)


def tables():
    return FakeWarehouse({"hourly_features": HOURLY, "daily_price_summary": DAILY})


def exported_at():
    return datetime(2026, 10, 9, 5, 30, tzinfo=UTC)


def test_each_table_lands_as_parquet_at_a_fixed_key():
    store = FakeStore()
    warehouse = tables()

    export(warehouse, store)

    assert store.writes[:2] == ["gold/hourly_features.parquet", "gold/daily_price_summary.parquet"]
    assert warehouse.reads == [("hourly_features", "ts_utc"), ("daily_price_summary", "local_date")]
    data, content_type = store.objects["gold/hourly_features.parquet"]
    assert content_type == PARQUET
    assert pq.read_table(io.BytesIO(data)).equals(HOURLY)


DAILY_ONLY = {"daily_price_summary": "local_date"}


def test_rerun_replaces_the_previous_export():
    store = FakeStore()
    export(FakeWarehouse({"daily_price_summary": DAILY}), store, DAILY_ONLY)
    newer = pa.table({"local_date": [date(2026, 10, 10)], "negative_price_hours": [0]})

    export(FakeWarehouse({"daily_price_summary": newer}), store, DAILY_ONLY)

    data, _ = store.objects["gold/daily_price_summary.parquet"]
    assert pq.read_table(io.BytesIO(data)).equals(newer)


def test_manifest_is_written_last():
    store = FakeStore()

    export(tables(), store)

    assert store.writes[-1] == MANIFEST_KEY
    assert store.objects[MANIFEST_KEY][1] == "application/json"


def test_manifest_has_row_counts_and_latest_time():
    store = FakeStore()

    manifest = export(tables(), store, now=exported_at)

    assert json.loads(store.objects[MANIFEST_KEY][0]) == manifest
    assert manifest["version"] == 1
    assert manifest["exported_at"] == "2026-10-09T05:30:00+00:00"
    hourly = manifest["tables"]["hourly_features"]
    assert hourly["rows"] == 2
    assert hourly["max_time"] == "2026-10-08T23:00:00+00:00"
    assert manifest["tables"]["daily_price_summary"]["max_time"] == "2026-10-09"
    body = store.objects["gold/hourly_features.parquet"][0]
    assert hourly["sha256"] == hashlib.sha256(body).hexdigest()
    assert hourly["bytes"] == len(body)


def test_empty_table_has_no_latest_time():
    empty = DAILY.slice(0, 0)

    manifest = export(
        FakeWarehouse({"daily_price_summary": empty}),
        FakeStore(),
        {"daily_price_summary": "local_date"},
    )

    assert manifest["tables"]["daily_price_summary"]["rows"] == 0
    assert manifest["tables"]["daily_price_summary"]["max_time"] is None
