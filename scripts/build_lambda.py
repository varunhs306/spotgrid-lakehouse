"""Build the API's Lambda zip: the locked `api` dependency group plus the package source.

The zip is byte-for-byte reproducible (sorted entries, fixed timestamps), so Terraform sees
a code change only when the code or a pinned dependency changed.

    uv run python scripts/build_lambda.py        # -> build/api-lambda.zip
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PACKAGE = ROOT / "src" / "spotgrid_lakehouse"
# Must match the function's runtime and architecture in infra/api.tf.
PYTHON = "3.13"
PLATFORM = "aarch64-manylinux_2_28"
FIXED_TIME = (1980, 1, 1, 0, 0, 0)


def install(target: Path) -> None:
    requirements = target.parent / "requirements.txt"
    subprocess.run(
        ["uv", "export", "--frozen", "--only-group", "api", "--no-emit-project", "--no-hashes",
         "--quiet", "--output-file", str(requirements)],
        check=True,
        cwd=ROOT,
    )  # fmt: skip
    subprocess.run(
        ["uv", "pip", "install", "--quiet", "--requirements", str(requirements),
         "--target", str(target), "--python-version", PYTHON, "--python-platform", PLATFORM,
         "--only-binary", ":all:"],
        check=True,
    )  # fmt: skip
    shutil.copytree(
        PACKAGE, target / PACKAGE.name, ignore=shutil.ignore_patterns("__pycache__", "*.pyc")
    )


def write_zip(source: Path, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(p for p in source.rglob("*") if p.is_file()):
            info = zipfile.ZipInfo(path.relative_to(source).as_posix(), FIXED_TIME)
            info.external_attr = 0o644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Build the API Lambda zip.")
    parser.add_argument("--output", type=Path, default=ROOT / "build" / "api-lambda.zip")
    args = parser.parse_args(argv)

    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "package"
        install(target)
        unpacked = sum(p.stat().st_size for p in target.rglob("*") if p.is_file())
        write_zip(target, args.output)
    zipped = args.output.stat().st_size
    print(f"{args.output}: {zipped / 1e6:.1f} MB, {unpacked / 1e6:.1f} MB unzipped")


if __name__ == "__main__":
    main()
