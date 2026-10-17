# Data SLA

Best effort, kept by the daily pipeline and checked on every run. Times are Europe/Berlin.

| Data | In the API |
|---|---|
| Tomorrow's day-ahead prices | Within an hour of SMARD publishing them; SMARD is polled every 30 minutes in the afternoon |
| Yesterday's load, wind and solar actuals | By 08:00 |
| Forecasts | By 08:00 for today, with tomorrow's prices for tomorrow |
| Revisions SMARD makes | Last 2 weeks: next day. Last 12 months: by Sunday 08:00 |

## Checks

- A failed or timed-out step opens an `incident` issue; the next good run closes it.
- Missing hours and changed published prices open `data-quality` issues, closed once fixed.
- dbt fails the build on a gap, an implausible value, or silver unchanged for 50 hours (warns at 26).
- `/v1/freshness` shows when the data was exported and how far each table reaches.

## Stability

- `gold.hourly_features` has an enforced contract, published in `contracts/hourly_features.json`.
  Columns may be added; any other change breaks CI.
- `_manifest.json` follows `contracts/manifest.schema.json`; its `version` changes with any change
  readers must handle.
- A full snapshot with checksums is released on the 1st of each month.

The API reads only the exported files, so a warehouse or pipeline outage leaves it serving the last
good export.
