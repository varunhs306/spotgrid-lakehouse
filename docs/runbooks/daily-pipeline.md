# Daily pipeline

`.github/workflows/daily.yml` runs once a day at 15:17 UTC, after the day-ahead auction results
are out, and can be started by hand:

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
   ```

## When a step fails

Each step is safe to rerun, so rerun the workflow once the cause is fixed.

| Step | Check |
|---|---|
| land bronze | SMARD reachable? A rerun uploads only chunks whose bytes changed |
| merge silver | The run URL printed by `bundle run`; replaying chunks changes nothing |
| build gold | The failing dbt test names the column. The export is skipped, so the API keeps the last good data |
| export | The API keeps serving the previous export; `_manifest.json` is written last |
