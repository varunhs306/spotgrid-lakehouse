# Gold export

The `export` workflow copies the gold tables from the SQL warehouse to the private serving
bucket, so the public API reads S3 and never wakes the warehouse.

```
s3://spotgrid-serving-<account id>/
  gold/hourly_features.parquet
  gold/daily_price_summary.parquet
  _manifest.json        rows, latest time, sha256 and size per table; written last
```

Each run overwrites the same keys. Bucket versioning keeps a week of older exports, so a bad
export can be rolled back by restoring the previous object versions.

## Run locally

```sh
export AWS_PROFILE=<profile that can write the bucket>
uv run python -m spotgrid_lakehouse.export.gold --profile spotlake \
  --bucket spotgrid-serving-<account id> --http-path /sql/1.0/warehouses/<id>
```

## Set up CI (once)

CI reads Databricks as a service principal and writes S3 as `spotgrid-export`, a role only
workflows on `main` can assume (`infra/export_role.tf`).

1. In the workspace, add a service principal (Settings → Identity and access) and create an
   OAuth secret for it.
2. Give it read access to gold and use of the warehouse:

   ```sql
   GRANT USE CATALOG ON CATALOG workspace TO `<application id>`;
   GRANT USE SCHEMA, SELECT ON SCHEMA workspace.gold TO `<application id>`;
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

Run it with `gh workflow run export.yml --ref main`, then check `_manifest.json` in the bucket.
