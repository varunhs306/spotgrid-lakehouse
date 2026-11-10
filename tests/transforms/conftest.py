import json

import pytest

from spotgrid_lakehouse.landing.volume import LocalVolume
from spotgrid_lakehouse.transforms.smard import connect


class Bronze(LocalVolume):
    def land(
        self,
        series: list,
        fetch_date: str = "2026-10-07",
        clock: str = "210000",
        path: str = "smard/4169/DE-LU/hour",
        week: int = 1790546400000,
        raw: bytes | None = None,
    ) -> str:
        """Write one chunk as landing does; `raw` replaces the JSON body."""
        series_id, region, resolution = path.split("/")[1:4]
        name = f"{series_id}_{region}_{resolution}_{week}_{clock}.json"
        key = f"{path}/fetch_date={fetch_date}/{name}"
        body = raw if raw is not None else json.dumps({"meta_data": {}, "series": series}).encode()
        self.write(key, body)
        self.write(f"{path}/fetch_date={fetch_date}/_manifest.json", b"{}")
        self.write(f"{path}/_index.json", b"{}")
        return key


@pytest.fixture
def bronze(tmp_path):
    return Bronze(tmp_path / "bronze")


@pytest.fixture
def warehouse():
    con = connect()
    yield con
    con.close()
