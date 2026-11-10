-- Every hour between a series' first and last hourly slot in silver is present. A gap means a
-- chunk failed to land or parse; the newest slots are not checked, as they grow with each run.
-- Warns only: SMARD sometimes serves nulls for a whole day, and that must not hold gold back.
{{ config(severity="warn") }}

with series as (
    select series_id, region, min(ts_utc) as first_ts, max(ts_utc) as last_ts
    from {{ source("silver", "smard_timeseries") }}
    where resolution = 'hour'
    group by all
),

expected as (
    select series_id, region, unnest(generate_series(first_ts, last_ts, interval 1 hour)) as ts_utc
    from series
)

select e.series_id, e.region, e.ts_utc
from expected as e
where not exists (
    select 1 from {{ source("silver", "smard_timeseries") }} as s
    where s.series_id = e.series_id
        and s.region = e.region
        and s.resolution = 'hour'
        and s.ts_utc = e.ts_utc
)
    -- Known gap in SMARD's offshore wind forecast: no values on 2015-01-04, local time.
    and not (
        e.series_id = 3791
        and e.ts_utc >= timestamptz '2015-01-03 23:00:00+00'
        and e.ts_utc < timestamptz '2015-01-04 23:00:00+00'
    )
