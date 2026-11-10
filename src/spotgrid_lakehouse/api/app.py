"""Public read-only API over the gold export.

Every response is built from the Parquet files in the serving bucket, never the warehouse, so
traffic costs Lambda time and S3 reads only.
"""

from __future__ import annotations

import gzip
import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Annotated

import pyarrow as pa
import pyarrow.compute as pc
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from spotgrid_lakehouse.api.data import GoldData, NoExport
from spotgrid_lakehouse.export.gold import ATTRIBUTION, cell, to_csv
from spotgrid_lakehouse.sources.catalog import GRID_REGIONS, SERIES, SERVED_ZONE, ZONES, Kind

# The export runs a few times a day; five minutes of client caching costs nothing in freshness.
CACHE_CONTROL = "public, max-age=300"
# Lambda returns at most 6 MB, and a gzipped body grows by a third in base64. Requests over
# this many values are asked to pick fields or use /v2/bulk.
MAX_CELLS = 400_000
MAX_BODY = 5_500_000
MAX_GZIP_BODY = 4_000_000
INTERNAL = {"updated_at"}


class Resolution(StrEnum):
    QUARTERHOUR = "15min"
    HOUR = "hour"
    DAY = "day"
    MONTH = "month"


class Format(StrEnum):
    JSON = "json"
    CSV = "csv"


SUFFIX = {
    Resolution.QUARTERHOUR: "quarterhour",
    Resolution.HOUR: "hourly",
    Resolution.DAY: "daily",
    Resolution.MONTH: "monthly",
}
# Without start and end, the newest stretch of this length that has data.
DEFAULT_SPAN = {
    Resolution.QUARTERHOUR: timedelta(days=7),
    Resolution.HOUR: timedelta(days=7),
    Resolution.DAY: timedelta(days=31),
    Resolution.MONTH: timedelta(days=366),
}
BULK_TABLES = {
    ("grid", Resolution.QUARTERHOUR), ("grid", Resolution.HOUR),
    ("prices", Resolution.QUARTERHOUR), ("prices", Resolution.HOUR),
}  # fmt: skip


def gold(request: Request) -> GoldData:
    return request.app.state.gold


Gold = Annotated[GoldData, Depends(gold)]
Start = Annotated[date | None, Query(description="First local date (Europe/Berlin), inclusive")]
End = Annotated[date | None, Query(description="Last local date (Europe/Berlin), inclusive")]
Res = Annotated[Resolution, Query(description="15min, hour, day or month")]
Fields = Annotated[
    str | None, Query(description="Comma-separated columns to return; keys are always included")
]
Fmt = Annotated[Format, Query(description="json or csv")]


def _list(value: str, allowed: tuple[str, ...], name: str) -> list[str]:
    items = [v.strip() for v in value.split(",") if v.strip()]
    unknown = [v for v in items if v not in allowed]
    if unknown or not items:
        raise HTTPException(422, f"unknown {name}: {', '.join(unknown) or '(none)'}; "
                                 f"choose from {', '.join(allowed)}")  # fmt: skip
    return list(dict.fromkeys(items))


def _window(
    data: GoldData, table: str, areas: list[str], start: date | None, end: date | None,
    resolution: Resolution,
) -> tuple[date, date] | None:  # fmt: skip
    """Resolve the requested local dates. A missing bound lies the default span from the
    other; with neither, the window ends on the latest date that has data.
    """
    span = DEFAULT_SPAN[resolution] - timedelta(days=1)
    if start is None and end is None:
        end = data.latest(table, areas)
        if end is None:
            return None
    if start is None:
        start = end - span
    if end is None:
        end = start + span
    if start > end:
        raise HTTPException(422, "start is after end")
    return start, end


def _select(
    data: GoldData, table: str, area_column: str, areas: list[str], start: date | None,
    end: date | None, resolution: Resolution, fields: str | None,
) -> tuple[pa.Table, tuple[date, date] | None]:  # fmt: skip
    window = _window(data, table, areas, start, end, resolution)
    columns = [c for c in data.columns(table) if c not in INTERNAL]
    date_column = "month" if resolution == Resolution.MONTH else "local_date"
    keys = [c for c in (area_column, "ts_utc", date_column) if c in columns]
    if fields:
        values = [c for c in columns if c not in keys]
        columns = keys + _list(fields, tuple(values), "fields")
    if window is None:
        return pa.table({c: [] for c in columns}), None
    first, last = window
    rows = data.read(table, areas, first, last)
    if rows.num_rows:
        dates = rows[date_column]
        lower = first.replace(day=1) if resolution == Resolution.MONTH else first
        mask = pc.and_(
            pc.and_(
                pc.greater_equal(dates, pa.scalar(lower)), pc.less_equal(dates, pa.scalar(last))
            ),
            pc.is_in(rows[area_column], pa.array(areas)),
        )
        rows = rows.filter(mask)
        order = [
            (area_column, "ascending"),
            (keys[1] if len(keys) > 1 else date_column, "ascending"),
        ]
        rows = rows.sort_by(order)
    return rows.select(columns), window


def _json_default(value):
    if isinstance(value, datetime | date):
        return cell(value)
    raise TypeError(type(value).__name__)


def respond(request: Request, rows: pa.Table, fmt: Format, meta: dict) -> Response:
    """The rows as JSON or CSV, gzipped when the client accepts it, within Lambda's limit."""
    if rows.num_rows * max(rows.num_columns, 1) > MAX_CELLS:
        raise HTTPException(
            413,
            f"{rows.num_rows} rows x {rows.num_columns} columns is too large for one response; "
            "narrow the dates, pick fields, or download whole months from /v2/bulk",
        )
    if fmt == Format.CSV:
        body, media = to_csv(rows), "text/csv; charset=utf-8"
    else:
        document = {"attribution": ATTRIBUTION, **meta, "rows": rows.to_pylist()}
        body = json.dumps(document, default=_json_default, separators=(",", ":")).encode()
        media = "application/json"
    headers = {
        "Cache-Control": CACHE_CONTROL,
        "X-Attribution": ATTRIBUTION,
        "Vary": "Accept-Encoding",
    }
    limit = MAX_BODY
    if "gzip" in request.headers.get("accept-encoding", ""):
        body, limit = gzip.compress(body, compresslevel=6), MAX_GZIP_BODY
        headers["Content-Encoding"] = "gzip"
    if len(body) > limit:
        raise HTTPException(413, "response too large; pick fields or use /v2/bulk")
    return Response(body, media_type=media, headers=headers)


router = APIRouter(prefix="/v2")


@router.get(
    "/grid",
    summary="Load, generation by source, forecasts and derived features per grid region",
    description="Regions: " + ", ".join(GRID_REGIONS) + ". A series SMARD does not publish "
    "for a region is null. Energy in MWh per slot; day and month sum the hours.",
)
def grid(
    request: Request, data: Gold, region: str = "DE", resolution: Res = Resolution.HOUR,
    start: Start = None, end: End = None, fields: Fields = None, format: Fmt = Format.JSON,
) -> Response:  # fmt: skip
    regions = _list(region, GRID_REGIONS, "region")
    rows, window = _select(
        data, f"grid_{SUFFIX[resolution]}", "region", regions, start, end, resolution, fields
    )
    return respond(request, rows, format, _meta(resolution, window))


@router.get(
    "/prices",
    summary="Day-ahead prices per bidding zone, with the spread to DE-LU",
    description="Zones: " + ", ".join(ZONES) + ". DE-LU includes DE-AT-LU until 2018-09-30. "
    "Day and month give base, peak (08:00-20:00, Monday to Friday) and off-peak prices.",
)
def prices(
    request: Request, data: Gold, zone: str = "DE-LU", resolution: Res = Resolution.HOUR,
    start: Start = None, end: End = None, fields: Fields = None, format: Fmt = Format.JSON,
) -> Response:  # fmt: skip
    zones = _list(zone, ZONES, "zone")
    rows, window = _select(
        data, f"prices_{SUFFIX[resolution]}", "zone", zones, start, end, resolution, fields
    )
    return respond(request, rows, format, _meta(resolution, window))


@router.get(
    "/revisions",
    summary="Changes SMARD made to published values",
    description="One row per change: the old and new value, and how long after the slot it "
    "was seen. Filter by series column (e.g. load_mwh), area (region or zone) and slot dates.",
)
def revisions(
    request: Request, data: Gold, series: str | None = None, area: str | None = None,
    start: Start = None, end: End = None, format: Fmt = Format.JSON,
) -> Response:  # fmt: skip
    end = end or date.today()
    start = start or end - timedelta(days=30)
    if start > end:
        raise HTTPException(422, "start is after end")
    rows = data.read("revisions", None, start, end)
    if rows.num_rows:
        mask = pc.and_(
            pc.greater_equal(rows["local_date"], pa.scalar(start)),
            pc.less_equal(rows["local_date"], pa.scalar(end)),
        )
        if series:
            mask = pc.and_(mask, pc.is_in(rows["series"], pa.array(series.split(","))))
        if area:
            mask = pc.and_(mask, pc.is_in(rows["area"], pa.array(area.split(","))))
        rows = rows.filter(mask).sort_by([("detected_at", "ascending"), ("ts_utc", "ascending")])
    return respond(request, rows, format, {"start": start, "end": end})


@router.get(
    "/catalog", summary="Every series, region and zone, how far each reaches, and freshness"
)
def catalog(request: Request, data: Gold) -> JSONResponse:
    manifest = data.manifest()
    exported_at = datetime.fromisoformat(manifest["exported_at"])
    age = request.app.state.now() - exported_at
    coverage = data.read("coverage", None).to_pylist()
    series = [
        {
            "id": s.id,
            "column": s.column,
            "label": s.label,
            "kind": str(s.kind),
            "unit": s.unit,
            **(
                {"zone": SERVED_ZONE.get(s.id, s.zone), "source_zone": s.zone}
                if s.kind == Kind.PRICE
                else {"regions": list(s.regions)}
            ),
            **({"ended": s.ended} if s.ended else {}),
        }
        for s in SERIES
    ]
    body = {
        "attribution": manifest["attribution"],
        "exported_at": manifest["exported_at"],
        "age_minutes": int(age.total_seconds() // 60),
        "regions": list(GRID_REGIONS),
        "zones": list(ZONES),
        "resolutions": [r.value for r in Resolution],
        "formats": [f.value for f in Format],
        "series": series,
        "coverage": coverage,
        "tables": {
            name: {"rows": t["rows"], "max_time": t["max_time"], "columns": t["columns"]}
            for name, t in manifest["tables"].items()
        },
    }
    return JSONResponse(json.loads(json.dumps(body, default=_json_default)),
                        headers={"Cache-Control": CACHE_CONTROL})  # fmt: skip


@router.get(
    "/bulk",
    summary="Download links for whole months as CSV",
    description="dataset grid or prices, resolution 15min or hour, one area; from and to are "
    "months (YYYY-MM). Links are valid for an hour.",
)
def bulk(
    request: Request, data: Gold, dataset: str = "grid", resolution: Res = Resolution.HOUR,
    area: str = "DE", start: Annotated[str | None, Query(alias="from")] = None,
    end: Annotated[str | None, Query(alias="to")] = None,
) -> JSONResponse:  # fmt: skip
    if (dataset, resolution) not in BULK_TABLES:
        raise HTTPException(422, "bulk files exist for grid and prices at 15min and hour")
    table = f"{dataset}_{SUFFIX[resolution]}"
    months = {
        name.rpartition("/")[2]: part
        for name, part in data.entry(table)["bulk"].items()
        if name.rpartition("/")[0] == area
    }
    if not months:
        raise HTTPException(404, f"no bulk files for {dataset} in {area}")
    first, last = start or min(months), end or max(months)
    files = [
        {
            "month": month,
            "rows": part["rows"],
            "bytes": part["bytes"],
            "sha256": part["sha256"],
            "url": request.app.state.link(part["key"]),
        }
        for month, part in sorted(months.items())
        if first <= month <= last
    ]
    return JSONResponse(
        {"attribution": ATTRIBUTION, "dataset": dataset, "resolution": resolution.value,
         "area": area, "files": files},
        headers={"Cache-Control": "no-store"},
    )  # fmt: skip


def _meta(resolution: Resolution, window: tuple[date, date] | None) -> dict:
    first, last = window or (None, None)
    return {"resolution": resolution.value, "start": first, "end": last}


def create_app(
    data: GoldData,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    link: Callable[[str], str] = lambda key: key,
) -> FastAPI:
    app = FastAPI(
        title="spotgrid",
        summary="German and neighbouring power-market data from SMARD, cleaned and joined",
        description=(
            f"Data: {ATTRIBUTION}. Dates are local to Europe/Berlin; times are UTC. "
            "Provided as is, without warranty of correctness or completeness."
        ),
        version="2",
    )
    app.state.gold = data
    app.state.now = now
    app.state.link = link
    app.include_router(router)

    @app.get("/", include_in_schema=False)
    def index() -> dict:
        return {"attribution": ATTRIBUTION, "docs": "/docs", "openapi": "/openapi.json"}

    @app.exception_handler(NoExport)
    def no_export(request: Request, exc: NoExport) -> JSONResponse:
        return JSONResponse({"detail": "no export published yet"}, status_code=503)

    return app
