"""Run dbt on the warehouse with the series catalog as vars.

    python -m spotgrid_lakehouse.gold build
    python -m spotgrid_lakehouse.gold source freshness

dbt runs in its own process: it keeps the DuckDB file open until it exits.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from spotgrid_lakehouse.sources.catalog import dbt_vars

PROJECT = Path(os.environ.get("DBT_PROJECT_DIR", Path(__file__).resolve().parents[2] / "dbt"))
RUNNER = "from dbt.cli.main import cli; cli()"


def dbt(args: list[str], warehouse: str | None = None, project: Path = PROJECT) -> bool:
    """Run one dbt command; True when it succeeded."""
    env = dict(os.environ)
    if warehouse:
        env["WAREHOUSE"] = str(Path(warehouse).resolve())
    command = [
        sys.executable, "-c", RUNNER, *args,
        "--project-dir", str(project),
        "--profiles-dir", str(project),
        "--vars", json.dumps(dbt_vars()),
    ]  # fmt: skip
    return subprocess.run(command, env=env, check=False).returncode == 0


def main(argv: list[str] | None = None) -> None:
    sys.exit(0 if dbt(list(sys.argv[1:] if argv is None else argv)) else 1)


if __name__ == "__main__":
    main()
