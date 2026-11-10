"""Gold exported to a directory laid out like the serving bucket."""

import io
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import jsonschema
import pyarrow.parquet as pq
import pytest

from spotgrid_lakehouse.export.gold import (
    MANIFEST_KEY,
    MANIFEST_VERSION,
    LocalStore,
    cell,
    export,
    snapshot,
)

SCHEMA = json.loads(
    (Path(__file__).parents[2] / "contracts" / "manifest.schema.json").read_text(encoding="utf-8")
)
EXPORTED = datetime(2026, 10, 26, 7, tzinfo=UTC)


class CountingStore(LocalStore):
    def __init__(self, root):
        super().__init__(root)
        self.writes = []

    def put(self, key, data, content_type):
        super().put(key, data, content_type)
        self.writes.append(key)


@pytest.fixture
def gold(built):
    con = duckdb.connect(built.path, read_only=True)
    con.execute("SET TimeZone = 'UTC'")
    yield con
    con.close()


@pytest.fixture
def store(tmp_path):
    return CountingStore(tmp_path / "serving")


def test_manifest_matches_the_schema(gold, store):
    manifest = export(gold, store, now=lambda: EXPORTED)

    jsonschema.validate(manifest, SCHEMA, format_checker=jsonschema.FormatChecker())
    assert SCHEMA["properties"]["version"]["const"] == MANIFEST_VERSION
    assert json.loads(store.get(MANIFEST_KEY)) == manifest
    assert store.writes[-1] == MANIFEST_KEY


def test_slot_tables_are_split_by_area_and_year(gold, store):
    manifest = export(gold, store, now=lambda: EXPORTED)

    hourly = manifest["tables"]["grid_hourly"]
    assert sorted(hourly["partitions"]) == sorted(
        f"{r}/2026"
        for r in ("DE", "AT", "LU", "50Hertz", "Amprion", "TenneT", "TransnetBW", "APG", "Creos")
    )
    part = hourly["partitions"]["DE/2026"]
    table = pq.read_table(io.BytesIO(store.get(part["key"])))
    assert table.num_rows == part["rows"] == 49
    assert set(table["region"].to_pylist()) == {"DE"}
    assert hourly["rows"] == 49 * 9


def test_bulk_csv_per_area_and_month(gold, store):
    manifest = export(gold, store, now=lambda: EXPORTED)

    entry = manifest["tables"]["prices_quarterhour"]["bulk"]["FR/2026-10"]
    lines = store.get(entry["key"]).decode().splitlines()
    assert lines[0].startswith("zone,ts_utc,local_date,price_eur_mwh,spread_to_de_lu_eur_mwh")
    assert lines[1].startswith("FR,2026-10-23T22:00:00Z,2026-10-24,")
    assert len(lines) == 1 + 49 * 4
    assert (
        "bulk" not in manifest["tables"]["grid_daily"]
        or not manifest["tables"]["grid_daily"]["bulk"]
    )


def test_unchanged_partitions_are_not_rewritten(gold, store):
    export(gold, store, now=lambda: EXPORTED)
    store.writes.clear()

    manifest = export(gold, store, now=lambda: EXPORTED)

    # Nothing changed: only the small unpartitioned tables, revisions and the manifest.
    assert all(not k.startswith(("gold/grid_", "gold/prices_", "bulk/")) for k in store.writes)
    assert manifest["tables"]["grid_hourly"]["rows"] == 49 * 9


def test_csv_cells():
    assert cell(datetime(2026, 1, 1, 23, tzinfo=UTC)) == "2026-01-01T23:00:00Z"
    assert (cell(None), cell(True), cell(1.5)) == ("", "true", "1.5")


def test_snapshot_writes_one_file_per_table_with_checksums(gold, tmp_path):
    manifest = snapshot(gold, tmp_path / "snapshot", EXPORTED)

    sums = (tmp_path / "snapshot" / "SHA256SUMS").read_text().splitlines()
    assert len(manifest["files"]) == 11
    assert len(sums) == 12
    assert manifest["files"]["grid_hourly.parquet"]["rows"] == 49 * 9
