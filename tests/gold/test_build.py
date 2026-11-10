"""Gold built by dbt from a warehouse holding every catalog source for two local days."""

import json
import shutil
from datetime import date, timedelta

import duckdb
import pytest

from spotgrid_lakehouse.gold import dbt
from spotgrid_lakehouse.transforms.smard import connect, run


@pytest.fixture
def gold(built):
    con = duckdb.connect(built.path, read_only=True)
    con.execute("SET TimeZone = 'UTC'")
    yield con
    con.close()


def one(con, sql: str):
    return con.execute(sql).fetchone()


def test_every_table_is_built(gold):
    tables = {
        r[0]
        for r in gold.execute(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = 'gold'"
        ).fetchall()
    }
    assert tables == {
        "grid_quarterhour", "grid_hourly", "grid_daily", "grid_monthly",
        "prices_quarterhour", "prices_hourly", "prices_daily", "prices_monthly",
        "revisions", "revision_stats", "coverage",
    }  # fmt: skip


def test_grid_rows_hold_every_series_and_features(gold):
    row = gold.execute(
        "SELECT wind_onshore_mwh, forecast_wind_onshore_mwh, wind_onshore_forecast_error_mwh, "
        "is_complete FROM gold.grid_hourly WHERE region = 'DE' ORDER BY ts_utc LIMIT 1"
    ).fetchone()
    # 4067 and 123 in the fixture: 1000 + id % 13 * 10 + len("DE").
    assert row == (1112.0, 1062.0, 50.0, True)


def test_series_a_region_lacks_are_null_but_count_as_zero_in_sums(gold):
    offshore, renewable, parts = one(
        gold,
        "SELECT wind_offshore_mwh, renewable_mwh, wind_onshore_mwh + solar_mwh + hydro_mwh "
        "+ biomass_mwh + other_renewable_mwh FROM gold.grid_hourly "
        "WHERE region = 'AT' ORDER BY ts_utc LIMIT 1",
    )
    assert offshore is None
    assert renewable == parts


def test_renewable_share_is_of_load(gold):
    share, renewable, load = one(
        gold,
        "SELECT renewable_share_pct, renewable_mwh, load_mwh FROM gold.grid_hourly "
        "WHERE region = 'DE' ORDER BY ts_utc LIMIT 1",
    )
    assert share == round(renewable / load * 100, 2)


def test_days_count_their_local_hours(gold):
    days = gold.execute(
        "SELECT local_date, hours, is_complete FROM gold.grid_daily WHERE region = 'DE' "
        "ORDER BY local_date"
    ).fetchall()
    # 2026-10-25 is the fall-back day.
    assert days == [(date(2026, 10, 24), 24, True), (date(2026, 10, 25), 25, True)]


def test_quarterhours_sum_to_the_hour(gold):
    quarters, hour = one(
        gold,
        "SELECT (SELECT sum(load_mwh) FROM gold.grid_quarterhour WHERE region = 'DE' "
        "AND date_trunc('hour', ts_utc) = h.ts_utc), h.load_mwh "
        "FROM gold.grid_hourly h WHERE region = 'DE' ORDER BY ts_utc LIMIT 1",
    )
    assert quarters == hour


def test_prices_carry_the_spread_to_de_lu(gold, built):
    spreads = dict(
        gold.execute(
            "SELECT zone, spread_to_de_lu_eur_mwh FROM gold.prices_hourly WHERE ts_utc = ?",
            [built.start],
        ).fetchall()
    )
    # 254 % 7 = 2 and 4169 % 7 = 4 in the fixture.
    assert spreads["DE-LU"] == 0
    assert spreads["FR"] == -2
    assert "DE-AT-LU" not in spreads
    assert len(spreads) == 14  # every zone but Poland; DE-AT-LU is part of DE-LU


def test_daily_prices(gold):
    row = one(
        gold,
        "SELECT price_hours, base_eur_mwh, peak_eur_mwh, negative_hours, range_eur_mwh "
        "FROM gold.prices_daily WHERE zone = 'DE-LU' AND local_date = DATE '2026-10-24'",
    )
    # Saturday: no peak hours.
    assert row[0] == 24 and row[2] is None and row[3] == 0
    # Highest 84 + 23 * 0.5, lowest 84 + 3 * 0.5 - 12.
    assert row[4] == pytest.approx(22.0)


def test_quarterhour_prices_are_marked_native(gold):
    marks = gold.execute(
        "SELECT DISTINCT source_resolution FROM gold.prices_quarterhour"
    ).fetchall()
    assert marks == [("quarterhour",)]


def test_coverage_names_each_series_and_area(gold):
    row = one(
        gold,
        "SELECT series, area, slots FROM gold.coverage WHERE series_id = 254 "
        "AND resolution = 'hour'",
    )
    assert row == ("price_eur_mwh", "FR", 49)


def test_incremental_run_updates_changed_slots_only(built, tmp_path):
    bronze = built.bronze
    copy = tmp_path / "warehouse.duckdb"
    shutil.copy(built.path, copy)
    later = built.landed + timedelta(days=1)
    start_ms = int(built.start.timestamp() * 1000)
    key = (
        f"smard/410/DE/hour/fetch_date={later:%Y-%m-%d}/410_DE_hour_{start_ms}_{later:%H%M%S}.json"
    )
    bronze.write(key, json.dumps({"series": [[start_ms, 31234.0]]}).encode())

    con = connect(str(copy))
    metrics = run(con, bronze, timedelta(days=1), now=later)
    con.close()
    assert metrics["updated"] == 1
    assert dbt(["build", "--quiet", "--select", "grid_hourly"], warehouse=str(copy))

    con = duckdb.connect(str(copy), read_only=True)
    load, rows = one(
        con,
        "SELECT (SELECT load_mwh FROM gold.grid_hourly WHERE region = 'DE' "
        "ORDER BY ts_utc LIMIT 1), (SELECT count(*) FROM gold.grid_hourly)",
    )
    con.close()
    assert load == 31234.0
    assert rows == 49 * 9
