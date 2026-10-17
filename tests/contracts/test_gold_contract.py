"""The published schema of gold.hourly_features, which the forecast project reads.

A column may be added. Dropping, renaming, retyping or making one nullable is a breaking
change: it needs a new contracts/hourly_features.json, and so a deliberate review.
"""

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]
SNAPSHOT = ROOT / "contracts" / "hourly_features.json"
GOLD_YML = ROOT / "dbt" / "models" / "gold" / "gold.yml"


def model(name: str) -> dict:
    models = yaml.safe_load(GOLD_YML.read_text(encoding="utf-8"))["models"]
    return next(m for m in models if m["name"] == name)


def contract_columns() -> dict[str, dict]:
    return {
        c["name"]: {
            "type": c.get("data_type"),
            "nullable": not any(k.get("type") == "not_null" for k in c.get("constraints", [])),
        }
        for c in model("hourly_features")["columns"]
    }


def test_contract_is_enforced():
    assert model("hourly_features")["config"]["contract"]["enforced"] is True


def test_every_column_has_a_type():
    assert all(c["type"] for c in contract_columns().values())


def test_published_columns_keep_their_name_type_and_nullability():
    published = json.loads(SNAPSHOT.read_text(encoding="utf-8"))["columns"]
    current = contract_columns()

    broken = [
        c["name"]
        for c in published
        if current.get(c["name"]) != {"type": c["type"], "nullable": c["nullable"]}
    ]
    assert broken == [], f"breaking change to published columns: {broken}"
