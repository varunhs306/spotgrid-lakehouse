import io
from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from spotgrid_lakehouse.export.gold import PARQUET, export

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

    def put(self, key, data, content_type):
        self.objects[key] = (data, content_type)


def test_each_table_lands_as_parquet_at_a_fixed_key():
    store = FakeStore()
    warehouse = FakeWarehouse({"hourly_features": HOURLY, "daily_price_summary": DAILY})

    keys = export(warehouse, store)

    assert keys == ["gold/hourly_features.parquet", "gold/daily_price_summary.parquet"]
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
