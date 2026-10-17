import json
from pathlib import Path

import jsonschema
import pytest
from test_gold_export import FakeStore, exported_at, tables

from spotgrid_lakehouse.export.gold import MANIFEST_VERSION, export

SCHEMA = json.loads(
    (Path(__file__).parents[2] / "contracts" / "manifest.schema.json").read_text(encoding="utf-8")
)


def manifest() -> dict:
    return export(tables(), FakeStore(), now=exported_at)


def test_export_writes_a_manifest_that_matches_the_schema():
    jsonschema.validate(manifest(), SCHEMA, format_checker=jsonschema.FormatChecker())


def test_schema_describes_the_current_version():
    assert SCHEMA["properties"]["version"]["const"] == MANIFEST_VERSION


@pytest.mark.parametrize(
    "corrupt",
    [
        lambda m: m.pop("attribution"),
        lambda m: m["tables"]["hourly_features"].pop("columns"),
        lambda m: m["tables"]["hourly_features"].update(sha256="not-a-hash"),
        lambda m: m.update(version=1),
    ],
)
def test_schema_rejects_a_broken_manifest(corrupt):
    broken = manifest()
    corrupt(broken)

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(broken, SCHEMA)
