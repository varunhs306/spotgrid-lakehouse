"""One pipeline run, as the scheduled Fargate task starts it.

    python -m spotgrid_lakehouse.pipeline run --mode morning
    python -m spotgrid_lakehouse.pipeline snapshot
    python -m spotgrid_lakehouse.pipeline contract
    python -m spotgrid_lakehouse.pipeline backfill --since 2015-01-01
    python -m spotgrid_lakehouse.pipeline first-run

A run holds a lock, downloads the warehouse file, lands bronze, and only with new data merges
silver, builds gold, exports and uploads the warehouse again. The report always goes out.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
import urllib.request
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

from spotgrid_lakehouse.landing import smard as landing
from spotgrid_lakehouse.landing.volume import open_volume
from spotgrid_lakehouse.sources.smard import Resolution, SmardClient

LOCK_TTL = timedelta(hours=6)
# Landing per mode: scope and weeks re-fetched. SMARD's corrections older than two weeks are
# not picked up; a backfill re-lands a period on demand.
MODES = {
    "morning": ("all", 2),
    "poll": ("prices", 2),
    "evening": ("all", 2),
    "manual": ("all", 2),
}


class Lake:
    """The warehouse file and the run lock: in S3, or in a directory for local runs."""

    def __init__(self, location: str) -> None:
        self.location = location.rstrip("/")
        self.s3 = None
        if self.location.startswith("s3://"):
            import boto3

            self.s3 = boto3.client("s3")
            self.bucket, _, self.prefix = self.location.removeprefix("s3://").partition("/")

    def _key(self, name: str) -> str:
        return f"{self.prefix}/{name}" if self.prefix else name

    def download(self, name: str, target: Path) -> bool:
        if self.s3 is None:
            source = Path(self.location) / name
            if not source.exists():
                return False
            shutil.copyfile(source, target)
            return True
        try:
            self.s3.download_file(self.bucket, self._key(name), str(target))
        except self.s3.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return False
            raise
        return True

    def upload(self, source: Path, name: str) -> None:
        if self.s3 is None:
            Path(self.location).mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, Path(self.location) / name)
        else:
            self.s3.upload_file(str(source), self.bucket, self._key(name))

    @contextmanager
    def lock(self, now: datetime):
        """Held for the run; a lock older than LOCK_TTL is taken over (its run died)."""
        body = json.dumps({"since": now.isoformat(), "by": os.environ.get("RUN_URL", "")})
        if not self._create("run.lock", body, now):
            raise Locked("another run holds the lock")
        try:
            yield
        finally:
            self._delete("run.lock")

    def _create(self, name: str, body: str, now: datetime) -> bool:
        if self.s3 is None:
            path = Path(self.location) / name
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists() and not self._stale(path.read_text(), now):
                return False
            path.write_text(body)
            return True
        try:
            self.s3.put_object(Bucket=self.bucket, Key=self._key(name), Body=body, IfNoneMatch="*")
            return True
        except self.s3.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] not in (
                "PreconditionFailed",
                "ConditionalRequestConflict",
            ):
                raise
        held = self.s3.get_object(Bucket=self.bucket, Key=self._key(name))
        if not self._stale(held["Body"].read().decode(), now):
            return False
        self.s3.put_object(Bucket=self.bucket, Key=self._key(name), Body=body)
        return True

    def _delete(self, name: str) -> None:
        if self.s3 is None:
            (Path(self.location) / name).unlink(missing_ok=True)
        else:
            self.s3.delete_object(Bucket=self.bucket, Key=self._key(name))

    @staticmethod
    def _stale(body: str, now: datetime) -> bool:
        try:
            return now - datetime.fromisoformat(json.loads(body)["since"]) > LOCK_TTL
        except (ValueError, KeyError):
            return True


class Locked(RuntimeError):
    pass


class Steps:
    """Outcome and duration of each step, for the report."""

    def __init__(self) -> None:
        self.outcomes: dict[str, str] = {}
        self.seconds: dict[str, int] = {}

    def run(self, name: str, action: Callable[[], object]) -> object | None:
        started = time.monotonic()
        try:
            result = action()
        except Exception as exc:
            print(f"step failed: {name}: {type(exc).__name__}: {exc}", flush=True)
            self.outcomes[name] = "failure"
            return None
        finally:
            self.seconds[name] = int(time.monotonic() - started)
        self.outcomes.setdefault(name, "success")
        return result

    def skip(self, *names: str) -> None:
        for name in names:
            self.outcomes[name] = "skipped"

    @property
    def failed(self) -> bool:
        return "failure" in self.outcomes.values()


def run_url() -> str:
    """CloudWatch link to this task's log stream, from the ECS task metadata."""
    base, group = os.environ.get("ECS_CONTAINER_METADATA_URI_V4"), os.environ.get("LOG_GROUP")
    region = os.environ.get("AWS_REGION")
    if not (base and group and region):
        return "(local run)"
    try:
        with urllib.request.urlopen(f"{base}/task", timeout=5) as response:
            task_id = json.loads(response.read())["TaskARN"].rsplit("/", 1)[1]
    except (OSError, ValueError, KeyError):
        return "(local run)"
    stream = f"pipeline/pipeline/{task_id}"

    def console(text: str) -> str:
        return quote(quote(text, safe=""), safe="").replace("%", "$")

    return (
        f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
        f"#logsV2:log-groups/log-group/{console(group)}/log-events/{console(stream)}"
    )


def heartbeat() -> None:
    """One success datapoint; an alarm fires when none arrives for a day."""
    namespace = os.environ.get("METRIC_NAMESPACE")
    if not namespace:
        return
    import boto3

    boto3.client("cloudwatch").put_metric_data(
        Namespace=namespace, MetricData=[{"MetricName": "PipelineSuccess", "Value": 1.0}]
    )


def new_data(report: dict | None, mode: str) -> bool:
    """A poll continues only for new DE-LU prices; other runs for any new chunk."""
    if mode == "manual":
        return True
    if report is None:
        return False
    landed = [e for e in report["landed"] if e["written"] > 0]
    if mode == "poll":
        landed = [e for e in landed if e["series"] == "PRICE_DE_LU"]
    return bool(landed)


def run(mode: str, env: dict, now: datetime | None = None) -> int:
    from spotgrid_lakehouse.export.gold import export, open_store
    from spotgrid_lakehouse.gold import PROJECT, dbt
    from spotgrid_lakehouse.monitoring import __main__ as report
    from spotgrid_lakehouse.monitoring.freshness import acknowledge
    from spotgrid_lakehouse.transforms import smard as silver

    started = now or datetime.now(UTC)
    scope, weeks = MODES[mode]
    lake, bronze = Lake(env["LAKE"]), open_volume(env["BRONZE"])
    work = Path(tempfile.mkdtemp(prefix="spotgrid-"))
    warehouse = work / "warehouse.duckdb"
    landing_file, manifest_file = work / "landing.json", work / "manifest.json"
    steps = Steps()
    gold_ok = False

    try:
        with lake.lock(started):
            lake.download("warehouse.duckdb", warehouse)

            def land() -> dict:
                selected = landing.selection(scope, list(Resolution))
                with SmardClient() as client:
                    try:
                        result = landing.run(bronze, client, selected, weeks, now=started)
                    except landing.LandingFailed as exc:
                        landing_file.write_text(json.dumps(exc.report, indent=2))
                        raise
                landing_file.write_text(json.dumps(result, indent=2))
                return result

            landed = steps.run("land bronze", land)
            if not new_data(landed, mode):
                steps.skip(
                    "merge silver", "build gold", "export", "record published source versions"
                )
            else:

                def merge() -> None:
                    con = silver.connect(str(warehouse), env.get("DUCKDB_MEMORY_LIMIT"))
                    try:
                        print(silver.run(con, bronze, timedelta(days=3), now=started), flush=True)
                    finally:
                        con.close()

                steps.run("merge silver", merge)

                def build() -> None:
                    # Source freshness catches a silver merge that succeeds but changes nothing.
                    if not dbt(["build"], warehouse=str(warehouse)):
                        raise RuntimeError("dbt build failed")
                    if not dbt(["source", "freshness"], warehouse=str(warehouse)):
                        raise RuntimeError("dbt source freshness failed")

                if steps.outcomes["merge silver"] == "success":
                    steps.run("build gold", build)
                    gold_ok = steps.outcomes["build gold"] == "success"
                else:
                    steps.skip("build gold")

                def publish() -> dict:
                    import duckdb

                    con = duckdb.connect(str(warehouse), read_only=True)
                    con.execute("SET TimeZone = 'UTC'")
                    try:
                        manifest = export(con, open_store(env["SERVING"]))
                    finally:
                        con.close()
                    manifest_file.write_text(json.dumps(manifest, indent=2))
                    return manifest

                if gold_ok:
                    manifest = steps.run("export", publish)
                    lake.upload(warehouse, "warehouse.duckdb")
                else:
                    manifest = None
                    steps.skip("export")

                def provenance() -> None:
                    document = json.loads(landing_file.read_text())
                    document["source_snapshot"] = acknowledge(
                        bronze, document["source_snapshot"], manifest
                    )
                    landing_file.write_text(json.dumps(document, indent=2))

                if manifest is not None:
                    steps.run("record published source versions", provenance)
                else:
                    steps.skip("record published source versions")
    except Locked as exc:
        print(f"skipped: {exc}", flush=True)
        return 0

    args = ["--started-at", started.isoformat(), "--landing", str(landing_file),
            "--manifest", str(manifest_file),
            "--dbt-results", str(PROJECT / "target" / "run_results.json")]  # fmt: skip
    if gold_ok:
        args += ["--warehouse", str(warehouse)]
    args += [f"--outcome={k}={v}" for k, v in steps.outcomes.items()]
    args += [f"--seconds={k}={v}" for k, v in steps.seconds.items()]
    report_env = {
        **env,
        "RUN_URL": env.get("RUN_URL") or run_url(),
        "QUIET_POLL": str(mode == "poll" and steps.outcomes.get("export") == "skipped").lower(),
    }
    report.main(args, env=report_env)
    shutil.rmtree(work, ignore_errors=True)
    if steps.failed:
        return 1
    heartbeat()
    return 0


def snapshot(env: dict, now: datetime | None = None) -> int:
    """The monthly data release: every gold table, checksummed, on GitHub Releases."""
    import duckdb

    from spotgrid_lakehouse.export.gold import snapshot as write_snapshot
    from spotgrid_lakehouse.release import publish

    now = now or datetime.now(UTC)
    work = Path(tempfile.mkdtemp(prefix="spotgrid-"))
    warehouse = work / "warehouse.duckdb"
    if not Lake(env["LAKE"]).download("warehouse.duckdb", warehouse):
        print("no warehouse yet", flush=True)
        return 1
    con = duckdb.connect(str(warehouse), read_only=True)
    con.execute("SET TimeZone = 'UTC'")
    try:
        manifest = write_snapshot(con, work / "snapshot", now)
    finally:
        con.close()
    tag = f"data-{now:%Y-%m}"
    notes = (
        f"Gold tables as of {manifest['exported_at']}. "
        f"Verify with `sha256sum -c SHA256SUMS`.\n\nData: {manifest['attribution']}"
    )
    publish(env["GITHUB_REPOSITORY"], env["GITHUB_TOKEN"], tag, f"Data snapshot {now:%Y-%m}",
            notes, sorted((work / "snapshot").iterdir()))  # fmt: skip
    shutil.rmtree(work, ignore_errors=True)
    heartbeat()
    return 0


def contract(env: dict) -> int:
    """SMARD's undocumented JSON still has the shape the pipeline reads, for every series."""
    from spotgrid_lakehouse.sources.smard import check_shapes

    with SmardClient() as client:
        problems = check_shapes(client)
    print("\n".join(problems) or "SMARD JSON shape as expected for every series", flush=True)
    if problems and (topic := env.get("ALARM_TOPIC_ARN")):
        import boto3

        boto3.client("sns").publish(
            TopicArn=topic,
            Subject="spotgrid: SMARD JSON shape changed",
            Message="\n".join(problems)[:200_000],
        )
    return 1 if problems else 0


def rebuild(env: dict, since: date | None = None, until: date | None = None) -> int:
    """Backfill bronze from `since`, or with no dates replay all of bronze; then rebuild silver
    with every revision bronze holds, gold in full, and a full export.
    """
    from spotgrid_lakehouse.export.gold import export, open_store
    from spotgrid_lakehouse.gold import dbt
    from spotgrid_lakehouse.transforms import smard as silver

    started = datetime.now(UTC)
    lake, bronze = Lake(env["LAKE"]), open_volume(env["BRONZE"])
    work = Path(tempfile.mkdtemp(prefix="spotgrid-"))
    warehouse = work / "warehouse.duckdb"
    with lake.lock(started):
        if since is not None:
            selected = landing.selection("all", list(Resolution))
            with SmardClient() as client:
                result = landing.run(
                    bronze, client, selected, since=since, until=until, now=started
                )
            written = sum(e["written"] for e in result["landed"])
            print(f"backfill: {len(result['landed'])} sources, {written} chunks", flush=True)
        con = silver.connect(str(warehouse), env.get("DUCKDB_MEMORY_LIMIT"))
        try:
            print(silver.rebuild(con, bronze, silver.SILVER_TABLE, with_revisions=True), flush=True)
        finally:
            con.close()
        if not dbt(["build", "--full-refresh"], warehouse=str(warehouse)):
            return 1
        import duckdb

        con = duckdb.connect(str(warehouse), read_only=True)
        con.execute("SET TimeZone = 'UTC'")
        try:
            export(con, open_store(env["SERVING"]), full=True)
        finally:
            con.close()
        lake.upload(warehouse, "warehouse.duckdb")
    shutil.rmtree(work, ignore_errors=True)
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m spotgrid_lakehouse.pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    run_cmd = commands.add_parser("run", help="a scheduled or manual run")
    run_cmd.add_argument("--mode", choices=list(MODES), default="manual")
    commands.add_parser("snapshot", help="release every gold table on GitHub")
    commands.add_parser("contract", help="check SMARD's JSON shape")
    backfill = commands.add_parser("backfill", help="land history, then rebuild everything")
    backfill.add_argument("--since", type=date.fromisoformat, required=True)
    backfill.add_argument("--until", type=date.fromisoformat)
    commands.add_parser("first-run", help="rebuild silver, gold and the export from bronze")
    args = parser.parse_args(argv)

    # Secrets not set yet hold the placeholder Terraform created.
    env = {k: v for k, v in os.environ.items() if v != "unset"}
    if args.command == "run":
        code = run(args.mode, {**env, "TRIGGER": env.get("TRIGGER", "manual")})
    elif args.command == "snapshot":
        code = snapshot(env)
    elif args.command == "contract":
        code = contract(env)
    elif args.command == "backfill":
        code = rebuild(env, args.since, args.until)
    else:
        code = rebuild(env)
    sys.exit(code)


if __name__ == "__main__":
    main()
