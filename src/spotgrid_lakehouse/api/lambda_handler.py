"""Lambda entrypoint: requests from the function URL, and warm-up pings from the scheduler."""

import os
from collections.abc import Callable

from mangum import Mangum

from spotgrid_lakehouse.api.app import create_app
from spotgrid_lakehouse.api.data import GoldData, NoExport, S3Source


def make_handler(data: GoldData) -> Callable[[dict, object], dict]:
    app = Mangum(create_app(data), lifespan="off")

    def handler(event: dict, context: object) -> dict:
        # The ping keeps a container alive with the latest tables loaded, so callers never wait
        # for a cold start or an S3 download.
        if event.get("warmup"):
            try:
                data.warm()
            except NoExport:
                return {"warm": False}
            return {"warm": True}
        return app(event, context)

    return handler


# Built once per container, so a warm Lambda keeps the tables in memory.
handler = make_handler(GoldData(S3Source(os.environ["SERVING_BUCKET"])))
