"""Lambda entrypoint: requests from the function URL, and warm-up pings from the scheduler."""

import os
from collections.abc import Callable

from mangum import Mangum

from spotgrid_lakehouse.api.app import create_app
from spotgrid_lakehouse.api.data import GoldData, NoExport, S3Source


def request(path: str) -> dict:
    """A GET as the function URL delivers it."""
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


def make_handler(data: GoldData) -> Callable[[dict, object], dict]:
    api = create_app(data)
    app = Mangum(api, lifespan="off")
    paths = list(api.openapi()["paths"])

    def handler(event: dict, context: object) -> dict:
        # The ping keeps a container alive with the latest tables loaded. It also sends each
        # endpoint one request, as FastAPI's first request in a container takes about 250 ms
        # longer than the rest; callers then never pay for it.
        if event.get("warmup"):
            try:
                data.warm()
            except NoExport:
                return {"warm": False}
            served = [app(request(path), context)["statusCode"] for path in paths]
            return {"warm": all(status == 200 for status in served)}
        return app(event, context)

    return handler


# Built once per container, so a warm Lambda keeps the tables in memory.
handler = make_handler(GoldData(S3Source(os.environ["SERVING_BUCKET"])))
