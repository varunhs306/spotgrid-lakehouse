# Databricks workspace setup

The pipeline uses a Databricks Free Edition workspace. Free Edition restricts outbound internet,
so source APIs are called from outside (laptop or CI) and the raw payloads are pushed into a
Unity Catalog volume through the Files API.

## Connect the CLI

```sh
databricks auth login --host https://<workspace>.cloud.databricks.com --profile spotlake
databricks current-user me
```

The Python SDK uses the same profile: pass `--profile spotlake`, set
`DATABRICKS_CONFIG_PROFILE`, or make it the default profile. With `auth_type = databricks-cli`
the `databricks` binary must be on `PATH`.

## Create the schemas and landing volume

Created by hand until Unity Catalog is managed by Terraform.

```sh
databricks schemas create bronze workspace --comment "Raw source payloads, unchanged"
databricks schemas create silver workspace --comment "Cleaned, merged source data"
databricks schemas create gold workspace --comment "Business-ready tables built by dbt"
databricks volumes create workspace bronze landing MANAGED \
  --comment "Raw API payloads landed by CI, with manifests"
```

## Land data

```sh
uv run python -m spotgrid_lakehouse.landing.smard --weeks 2
uv run python -m spotgrid_lakehouse.landing.smard --series PRICE_DE_LU LOAD --resolution hour quarterhour
```

Layout under `/Volumes/workspace/bronze/landing`:

```
smard/{series}/{region}/{resolution}/
  _index.json                       latest sha256 per chunk, across fetch dates
  fetch_date=YYYY-MM-DD/
    {series}_{region}_{resolution}_{start_ms}.json   raw payload, unchanged
    _manifest.json                  sha256, bytes, source URL, fetched_at per file
```

A chunk is uploaded only when its sha256 differs from `_index.json`, so a rerun uploads nothing
and a revised chunk lands again under the new fetch date. Writes go chunk → manifest → index;
an interrupted run is redone by the next one. One writer at a time.

Check: `databricks fs ls dbfs:/Volumes/workspace/bronze/landing/smard/4169/DE-LU/hour/`

## Build silver

The `silver_smard` job in `databricks.yml` merges landed chunks into
`workspace.silver.smard_timeseries`, one row per series, region, resolution and slot. Code ships
as a wheel to serverless compute.

```sh
databricks bundle deploy -p spotlake
databricks bundle run silver_smard -p spotlake              # chunks landed in the last 3 days
databricks bundle run silver_smard -p spotlake -- --all     # every chunk in bronze
```

The job prints the MERGE metrics. A value changes only when a newer landing disagrees, which bumps
`revision_count`; replaying older chunks changes nothing, so a rerun reports 0 inserted, 0 updated.

## Build gold

dbt runs against the SQL warehouse as a pinned tool, outside the project environment
(`dbt-databricks` caps `databricks-sdk` below what the pipeline needs).

```sh
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id>      # databricks warehouses list
export DATABRICKS_TOKEN=$(databricks auth token -p spotlake | jq -r .access_token)
cd dbt
uvx --from dbt-core==1.12.5 --with dbt-databricks==1.12.6 dbt build --profiles-dir .
```

In Git Bash on Windows, also set `MSYS2_ENV_CONV_EXCL=DATABRICKS_HTTP_PATH`, or the path is rewritten.
