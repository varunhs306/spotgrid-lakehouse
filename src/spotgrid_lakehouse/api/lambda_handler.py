"""Lambda entrypoint behind the function URL."""

import os

from mangum import Mangum

from spotgrid_lakehouse.api.app import create_app
from spotgrid_lakehouse.api.data import GoldData, S3Source

# Built once per container, so a warm Lambda keeps the tables in memory.
handler = Mangum(create_app(GoldData(S3Source(os.environ["SERVING_BUCKET"]))), lifespan="off")
