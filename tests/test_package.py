import spotgrid_lakehouse


def test_package_imports():
    assert spotgrid_lakehouse.__doc__


def test_lambda_code_needs_neither_duckdb_nor_dbt(monkeypatch):
    """The Lambda zip ships the api group only; the API and probe must import without them."""
    import importlib
    import sys

    monkeypatch.setitem(sys.modules, "duckdb", None)
    monkeypatch.setitem(sys.modules, "dbt", None)
    monkeypatch.setenv("SERVING_BUCKET", "bucket")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-central-1")
    for name in list(sys.modules):
        if name.startswith("spotgrid_lakehouse.api") or name == "spotgrid_lakehouse.export.gold":
            monkeypatch.delitem(sys.modules, name)
    importlib.import_module("spotgrid_lakehouse.api.lambda_handler")
    importlib.import_module("spotgrid_lakehouse.monitoring.probe")
