import copy
import json
from pathlib import Path

import pytest

from spotgrid_lakehouse.sources.smard import Resolution, shape_problems

FIXTURES = Path(__file__).parents[1] / "fixtures" / "smard" / "4169" / "DE-LU"
START = 1790546400000
INDEX = json.loads((FIXTURES / "index_hour.json").read_text())
CHUNK = json.loads((FIXTURES / f"4169_DE-LU_hour_{START}.json").read_text())


def problems(index=INDEX, chunk=CHUNK, resolution=Resolution.HOUR):
    return shape_problems(index, chunk, resolution, START)


def changed(edit):
    chunk = copy.deepcopy(CHUNK)
    edit(chunk)
    return chunk


def test_recorded_payloads_have_the_expected_shape():
    assert problems() == []


@pytest.mark.parametrize(
    ("chunk", "problem"),
    [
        (changed(lambda c: c.pop("series")), "'series' is missing"),
        (changed(lambda c: c.update(series={"data": []})), "'series' is missing"),
        (changed(lambda c: c["series"][0].append(1.0)), "not [timestamp, value] pairs"),
        (changed(lambda c: c["series"][0].__setitem__(0, "2026-09-28")), "not epoch milliseconds"),
        (changed(lambda c: c["series"].pop(5)), "do not run every hour"),
        (changed(lambda c: c["series"][3].__setitem__(1, "88.5")), "not numbers or null"),
        (changed(lambda c: [p.__setitem__(1, None) for p in c["series"]]), "every value is null"),
    ],
)
def test_changed_chunks_are_reported(chunk, problem):
    assert any(problem in p for p in problems(chunk=chunk))


def test_changed_index_is_reported():
    assert problems(index={"timestamps": "1790546400000"}) == [
        "index: 'timestamps' is not a list of epoch milliseconds"
    ]


def test_slot_length_follows_the_resolution():
    assert any("every quarterhour" in p for p in problems(resolution=Resolution.QUARTERHOUR))
