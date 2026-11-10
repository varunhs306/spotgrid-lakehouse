"""Lambda entrypoint: requests from API Gateway, and warm-up pings from the scheduler."""

import os
from collections.abc import Callable

from mangum import Mangum

from spotgrid_lakehouse.api.app import create_app
from spotgrid_lakehouse.api.data import GoldData, NoExport, S3Source

# What a request without parameters reads, loaded ahead of it.
WARM = [
    (f"{dataset}_{suffix}", area)
    for dataset, area in (("grid", "DE"), ("prices", "DE-LU"))
    for suffix in ("quarterhour", "hourly", "daily", "monthly")
]
WARM_PATHS = ["/v2/grid", "/v2/prices", "/v2/catalog"]


def request(path: str) -> dict:
    """A GET as API Gateway delivers it (payload 2.0)."""
    return {
        "version": "2.0",
        "routeKey": "$default",
        "rawPath": path,
        "rawQueryString": "",
        "headers": {"host": "warmup"},
        "requestContext": {
            "http": {"method": "GET", "path": path, "sourceIp": "127.0.0.1"},
            "stage": "$default",
        },
        "isBase64Encoded": False,
    }


def make_handler(data: GoldData, link: Callable[[str], str]) -> Callable[[dict, object], dict]:
    api = create_app(data, link=link)
    app = Mangum(api, lifespan="off")

    def handler(event: dict, context: object) -> dict:
        # The ping keeps a container alive with the newest partitions loaded, and sends the
        # main endpoints one request, as FastAPI's first request in a container is slower.
        if event.get("warmup"):
            try:
                data.warm(WARM)
            except NoExport:
                return {"warm": False}
            served = [app(request(path), context)["statusCode"] for path in WARM_PATHS]
            return {"warm": all(status == 200 for status in served)}
        return app(event, context)

    return handler


# Built once per container, so a warm Lambda keeps partitions in memory.
_source = S3Source(os.environ["SERVING_BUCKET"])
handler = make_handler(GoldData(_source), _source.url)
