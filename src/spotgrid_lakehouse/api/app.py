"""Public read-only API over the daily gold export.

Every response is built from the Parquet files in the serving bucket, never the SQL warehouse,
so traffic costs Lambda time and S3 reads only.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from functools import reduce
from typing import Annotated

import pyarrow as pa
import pyarrow.compute as pc
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from spotgrid_lakehouse.api.data import GoldData, NoExport
from spotgrid_lakehouse.export.gold import ATTRIBUTION

HOURLY = "hourly_features"
DAILY = "daily_price_summary"
DEFAULT_DAYS = 7
MAX_HOURLY_DAYS = 366
MAX_DAILY_DAYS = 3660
# The export runs once a day; five minutes of client caching costs nothing in freshness.
CACHE_CONTROL = "public, max-age=300"


class PriceRow(BaseModel):
    ts_utc: datetime
    local_date: date
    price_eur_mwh: float


class LoadRow(BaseModel):
    ts_utc: datetime
    local_date: date
    load_mwh: float
    residual_load_mwh: float | None


class GenerationRow(BaseModel):
    ts_utc: datetime
    local_date: date
    wind_onshore_mwh: float | None
    wind_offshore_mwh: float | None
    solar_mwh: float | None
    forecast_wind_onshore_mwh: float | None
    forecast_wind_offshore_mwh: float | None
    forecast_solar_mwh: float | None


class DailyPriceRow(BaseModel):
    local_date: date
    min_price_eur_mwh: float
    avg_price_eur_mwh: float
    max_price_eur_mwh: float
    negative_price_hours: int
    price_hours: int


class TableFreshness(BaseModel):
    rows: int
    max_time: str | None


class Freshness(BaseModel):
    attribution: str
    exported_at: datetime
    age_minutes: int
    tables: dict[str, TableFreshness]


class Rows[RowT: BaseModel](BaseModel):
    attribution: str
    start: date | None
    end: date | None
    rows: list[RowT]


def gold(request: Request) -> GoldData:
    return request.app.state.gold


Gold = Annotated[GoldData, Depends(gold)]
Start = Annotated[date | None, Query(description="First local date (Europe/Berlin), inclusive")]
End = Annotated[date | None, Query(description="Last local date (Europe/Berlin), inclusive")]


def window(
    dates: pa.ChunkedArray, start: date | None, end: date | None, max_days: int
) -> tuple[date, date] | None:
    """Resolve the requested local dates. A missing bound lies DEFAULT_DAYS from the other;
    with neither, the window ends on the latest date that has data.
    """
    if start is None and end is None:
        end = pc.max(dates).as_py()
        if end is None:
            return None
    span = timedelta(days=DEFAULT_DAYS - 1)
    if start is None:
        start = end - span
    if end is None:
        end = start + span
    if start > end:
        raise HTTPException(422, "start is after end")
    if (end - start).days >= max_days:
        raise HTTPException(422, f"at most {max_days} days per request")
    return start, end


def rows(
    table: pa.Table, model: type[BaseModel], start: date | None, end: date | None, max_days: int
) -> Rows:
    """Rows of `model`'s columns in the window, skipping rows where every value is null:
    the newest hours carry forecasts and prices but no actuals yet.
    """
    columns = list(model.model_fields)
    values = [pc.is_valid(table[c]) for c in columns if c not in ("ts_utc", "local_date")]
    table = table.select(columns).filter(reduce(pc.or_, values))
    bounds = window(table["local_date"], start, end, max_days)
    if bounds is None:
        return Rows[model](attribution=ATTRIBUTION, start=start, end=end, rows=[])
    first, last = bounds
    in_window = pc.and_(
        pc.greater_equal(table["local_date"], pa.scalar(first)),
        pc.less_equal(table["local_date"], pa.scalar(last)),
    )
    return Rows[model](
        attribution=ATTRIBUTION, start=first, end=last, rows=table.filter(in_window).to_pylist()
    )


router = APIRouter(prefix="/v1")


@router.get("/prices", summary="Hourly day-ahead prices, bidding zone DE-LU")
def prices(data: Gold, start: Start = None, end: End = None) -> Rows[PriceRow]:
    return rows(data.table(HOURLY), PriceRow, start, end, MAX_HOURLY_DAYS)


@router.get("/prices/daily", summary="Daily min, mean and max price and negative-price hours")
def daily_prices(data: Gold, start: Start = None, end: End = None) -> Rows[DailyPriceRow]:
    return rows(data.table(DAILY), DailyPriceRow, start, end, MAX_DAILY_DAYS)


@router.get("/load", summary="Hourly grid load and residual load")
def load(data: Gold, start: Start = None, end: End = None) -> Rows[LoadRow]:
    return rows(data.table(HOURLY), LoadRow, start, end, MAX_HOURLY_DAYS)


@router.get("/generation", summary="Hourly wind and solar generation, actual and forecast")
def generation(data: Gold, start: Start = None, end: End = None) -> Rows[GenerationRow]:
    return rows(data.table(HOURLY), GenerationRow, start, end, MAX_HOURLY_DAYS)


@router.get("/freshness", summary="When the data was last exported, and how far it reaches")
def freshness(data: Gold, request: Request) -> Freshness:
    manifest = data.manifest()
    exported_at = datetime.fromisoformat(manifest["exported_at"])
    age = request.app.state.now() - exported_at
    return Freshness(
        attribution=manifest["attribution"],
        exported_at=exported_at,
        age_minutes=int(age.total_seconds() // 60),
        tables=manifest["tables"],
    )


def create_app(data: GoldData, now: Callable[[], datetime] = lambda: datetime.now(UTC)) -> FastAPI:
    app = FastAPI(
        title="spotgrid",
        summary="German power-market data from the spotgrid lakehouse",
        description=f"Data: {ATTRIBUTION}. Dates are local to Europe/Berlin; times are UTC.",
        version="1",
    )
    app.state.gold = data
    app.state.now = now
    app.include_router(router)

    @app.get("/", include_in_schema=False)
    def index() -> dict:
        return {"attribution": ATTRIBUTION, "docs": "/docs", "openapi": "/openapi.json"}

    @app.exception_handler(NoExport)
    def no_export(request: Request, exc: NoExport) -> JSONResponse:
        return JSONResponse({"detail": "no export published yet"}, status_code=503)

    @app.middleware("http")
    async def cache_headers(request: Request, call_next):
        response = await call_next(request)
        if request.method == "GET" and response.status_code == 200:
            response.headers.setdefault("Cache-Control", CACHE_CONTROL)
        return response

    return app
