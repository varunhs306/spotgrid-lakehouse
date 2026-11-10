"""Every SMARD series the pipeline lands, with where SMARD publishes it.

Availability per region was read from SMARD's index files on 2026-11-10. A region missing
here returned 404 or an empty index.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

GRID_REGIONS = ("DE", "AT", "LU", "50Hertz", "Amprion", "TenneT", "TransnetBW", "APG", "Creos")
GERMAN_GRID = ("DE", "50Hertz", "Amprion", "TenneT", "TransnetBW")
ALL_BUT_LU = tuple(r for r in GRID_REGIONS if r not in ("LU", "Creos"))


class Kind(StrEnum):
    PRICE = "price"
    GENERATION = "generation"
    CONSUMPTION = "consumption"
    FORECAST = "forecast"


@dataclass(frozen=True)
class SeriesInfo:
    id: int
    column: str
    label: str
    kind: Kind
    # Grid regions for volumes; one bidding zone for prices.
    regions: tuple[str, ...]
    # Per hour, for the German series only; a quarter-hour holds a quarter for volumes.
    limits: tuple[float, float] | None = None
    # Prices only: SMARD's region path segment (prices ignore it), and the zone code served.
    zone: str | None = None
    path_region: str | None = None
    # Local day the series stopped; later slots are zero, not missing.
    ended: str | None = None

    @property
    def unit(self) -> str:
        return "EUR/MWh" if self.kind == Kind.PRICE else "MWh"


def _grid(id, column, label, kind, regions=GRID_REGIONS, limits=None, ended=None):
    return SeriesInfo(id, column, label, kind, regions, limits, ended=ended)


def _price(id, zone, label, limits=(-500, 4000), path_region="DE", ended=None):
    return SeriesInfo(
        id, "price_eur_mwh", label, Kind.PRICE, (zone,), limits, zone, path_region, ended
    )


GENERATION = Kind.GENERATION
FORECAST = Kind.FORECAST
SERIES: tuple[SeriesInfo, ...] = (
    # Generation by source.
    _grid(1223, "lignite_mwh", "Lignite", GENERATION, ("DE", "50Hertz", "Amprion", "TenneT")),
    # Germany's last reactors shut down on 2023-04-15.
    _grid(
        1224,
        "nuclear_mwh",
        "Nuclear",
        GENERATION,
        ("DE", "Amprion", "TenneT", "TransnetBW"),
        ended="2023-04-16",
    ),
    _grid(
        1225,
        "wind_offshore_mwh",
        "Wind offshore",
        GENERATION,
        ("DE", "50Hertz", "TenneT"),
        (0, 20000),
    ),
    _grid(1226, "hydro_mwh", "Hydropower", GENERATION),
    _grid(1227, "other_conventional_mwh", "Other conventional", GENERATION),
    _grid(1228, "other_renewable_mwh", "Other renewable", GENERATION, ALL_BUT_LU),
    _grid(4066, "biomass_mwh", "Biomass", GENERATION),
    _grid(4067, "wind_onshore_mwh", "Wind onshore", GENERATION, limits=(0, 80000)),
    _grid(4068, "solar_mwh", "Photovoltaics", GENERATION, limits=(0, 150000)),
    _grid(4069, "hard_coal_mwh", "Hard coal", GENERATION, ALL_BUT_LU),
    _grid(4070, "pumped_storage_mwh", "Pumped storage", GENERATION, ALL_BUT_LU),
    _grid(4071, "natural_gas_mwh", "Natural gas", GENERATION),
    # Consumption.
    _grid(410, "load_mwh", "Grid load", Kind.CONSUMPTION, limits=(20000, 100000)),
    _grid(4359, "residual_load_mwh", "Residual load", Kind.CONSUMPTION),
    _grid(
        4387,
        "pumped_storage_consumption_mwh",
        "Pumped storage consumption",
        Kind.CONSUMPTION,
        ALL_BUT_LU,
    ),
    # Forecasts.
    _grid(122, "forecast_total_mwh", "Forecast: total generation", FORECAST, ALL_BUT_LU),
    _grid(715, "forecast_other_mwh", "Forecast: other generation", FORECAST),
    _grid(5097, "forecast_wind_solar_mwh", "Forecast: wind and photovoltaics", FORECAST),
    _grid(123, "forecast_wind_onshore_mwh", "Forecast: wind onshore", FORECAST, limits=(0, 80000)),
    _grid(
        3791,
        "forecast_wind_offshore_mwh",
        "Forecast: wind offshore",
        FORECAST,
        ("DE", "50Hertz", "TenneT"),
        (0, 20000),
    ),
    _grid(125, "forecast_solar_mwh", "Forecast: photovoltaics", FORECAST, limits=(0, 150000)),
    # Day-ahead prices, one bidding zone each. SDAC clears between -500 and 4000 EUR/MWh.
    _price(4169, "DE-LU", "Germany/Luxembourg", path_region="DE-LU"),
    _price(
        251,
        "DE-AT-LU",
        "Germany/Austria/Luxembourg, until 2018-09-30",
        path_region="DE-AT-LU",
        ended="2018-10-01",
    ),
    _price(5078, "DE-LU-NEIGHBOURS", "Neighbours of DE/LU (SMARD aggregate)", limits=None),
    _price(4170, "AT", "Austria"),
    _price(4996, "BE", "Belgium"),
    _price(4997, "NO2", "Norway 2"),
    _price(252, "DK1", "Denmark 1"),
    _price(253, "DK2", "Denmark 2"),
    _price(254, "FR", "France"),
    _price(255, "IT-NORTH", "Italy North"),
    _price(256, "NL", "Netherlands"),
    # Switzerland is outside SDAC, so its clearing limits differ.
    _price(259, "CH", "Switzerland", limits=None),
    _price(260, "SI", "Slovenia"),
    _price(261, "CZ", "Czechia"),
    _price(262, "HU", "Hungary"),
    # 257 and 258 are both labelled "Poland" by SMARD and carry different values; left out
    # until it is known which is which.
)

BY_ID = {s.id: s for s in SERIES}
PRICES = tuple(s for s in SERIES if s.kind == Kind.PRICE)
GRID = tuple(s for s in SERIES if s.kind != Kind.PRICE)
# DE-AT-LU ends where DE-LU starts; gold serves them as one zone, DE-LU.
SERVED_ZONE = {251: "DE-LU"}
ZONES = tuple(dict.fromkeys(SERVED_ZONE.get(s.id, s.zone) for s in PRICES))


def dbt_vars() -> dict:
    """The catalog as dbt vars, so the SQL models follow it."""
    return {
        "grid": [
            {
                "id": s.id,
                "column": s.column,
                "kind": str(s.kind),
                "regions": list(s.regions),
                "limits": list(s.limits) if s.limits else None,
                "ended": s.ended,
            }
            for s in GRID
        ],
        "prices": [
            {
                "id": s.id,
                "zone": SERVED_ZONE.get(s.id, s.zone),
                "source_zone": s.zone,
                "limits": list(s.limits) if s.limits else None,
            }
            for s in PRICES
        ],
        "regions": list(GRID_REGIONS),
    }


def path_region(series: SeriesInfo, region: str) -> str:
    """The region segment of SMARD's URL and of the bronze path."""
    return series.path_region if series.kind == Kind.PRICE else region


def sources(kinds: set[Kind] | None = None) -> list[tuple[SeriesInfo, str]]:
    """Every (series, region) SMARD publishes, optionally only some kinds."""
    return [
        (s, path_region(s, r))
        for s in SERIES
        if kinds is None or s.kind in kinds
        for r in s.regions
    ]
