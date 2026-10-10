# Gold export

The export step copies the gold tables from the SQL warehouse to the private serving
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

## In CI

The export is the last step of the daily workflow; set-up and reruns are in
[daily-pipeline.md](daily-pipeline.md).
