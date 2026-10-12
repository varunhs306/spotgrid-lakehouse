# Daily pipeline

`.github/workflows/daily.yml` runs twice a day and can be started by hand:

- **05:17 UTC**: yesterday's actuals and today's forecasts
- **15:17 UTC**: tomorrow's day-ahead prices, out after the auction early in the afternoon

Each run:

1. **land bronze**: the latest two weekly SMARD chunks of every series into the landing volume
2. **merge silver**: deploys the asset bundle (`-t prod`) and runs `silver_smard`
3. **build gold**: `dbt build`, models and tests
4. **export**: gold to the serving bucket ([gold-export.md](gold-export.md)), which the API reads

Every step runs as one Databricks service principal; the export writes S3 as `spotgrid-export`,
a role only workflows on `main` can assume. The job runs only on `main`.

```sh
gh workflow run daily.yml --ref main
gh run watch
```

## Set up (once)

1. In the workspace, add a service principal (Settings → Identity and access) and create an
   OAuth secret for it.
2. Grant it the layers it writes. dbt replaces gold tables, which only their owner may do, so
   it also takes over the tables built by hand so far:

   ```sql
   GRANT USE CATALOG ON CATALOG workspace TO `<application id>`;
   GRANT USE SCHEMA, READ VOLUME, WRITE VOLUME ON SCHEMA workspace.bronze TO `<application id>`;
   GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA workspace.silver TO `<application id>`;
   GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA workspace.gold TO `<application id>`;
   ALTER TABLE workspace.gold.hourly_features OWNER TO `<application id>`;
   ALTER TABLE workspace.gold.daily_price_summary OWNER TO `<application id>`;
   ```

   and **Can use** on the SQL warehouse (warehouse → Permissions).
3. Hand the values to GitHub:

   ```sh
   gh variable set DATABRICKS_HOST --body https://<workspace>.cloud.databricks.com
   gh variable set DATABRICKS_HTTP_PATH --body /sql/1.0/warehouses/<id>
   gh variable set DATABRICKS_CLIENT_ID --body <application id>
   gh secret set DATABRICKS_CLIENT_SECRET
   gh variable set SERVING_BUCKET --body "$(terraform -chdir=infra output -raw serving_bucket)"
   gh variable set AWS_EXPORT_ROLE_ARN --body "$(terraform -chdir=infra output -raw export_role_arn)"
   gh variable set API_URL --body "$(terraform -chdir=infra output -raw api_url)"
   ```

## Monitoring

The last step (`python -m spotgrid_lakehouse.monitoring`) runs after failures too.

| Channel | When | What |
|---|---|---|
| Actions run summary | Every run | Step results and times, chunks landed with their local days, silver rows added and changed, dbt results, export, API check, irregularities |
| Telegram | Afternoon and manual runs; the morning run only on a problem | A digest (pipeline, completeness per series group, tomorrow's prices, API, open issues), plus a note when an issue opens or closes |
| GitHub issues | On a problem | `incident`: the run failed, closed by the next good run. `data-quality`: one issue per problem, closed when it is gone, with how long each day was affected |
| GitHub email | On a problem | GitHub's notifications for those issues and for failed runs |

Data-quality checks, on the 14 local days the run re-fetches:

| Check | Rule |
|---|---|
| Missing hours | Actuals complete up to yesterday, forecasts up to today, day-ahead prices up to tomorrow from 16:00 Berlin time. Days have 23, 24 or 25 hours |
| Changed prices | Any published day-ahead price that changed; they are final once the auction is out |

Data-quality issues only change after a successful run, so a failed run cannot close them.
Try it locally without alerting anyone:

```sh
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<id> API_URL=<api url>   uv run python -m spotgrid_lakehouse.monitoring --started-at <UTC time> --profile spotlake --dry-run
```

Telegram needs `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` as repository secrets (bot from
@BotFather; your chat id from @userinfobot). Without them the step skips Telegram.

## When a step fails

Each step is safe to rerun, so rerun the workflow once the cause is fixed.

| Step | Check |
|---|---|
| land bronze | SMARD reachable? A rerun uploads only chunks whose bytes changed |
| merge silver | The run URL printed by `bundle run`; replaying chunks changes nothing |
| build gold | The failing dbt test names the column. The export is skipped, so the API keeps the last good data |
| export | The API keeps serving the previous export; `_manifest.json` is written last |
