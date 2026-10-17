-- Every hour between a series' first and last hourly slot in silver is present. A gap means a
-- chunk failed to land or parse; the newest slots are not checked, as they grow with each run.
with series as (
    select series_id, region, min(ts_utc) as first_ts, max(ts_utc) as last_ts
    from {{ source("silver", "smard_timeseries") }}
    where resolution = 'hour'
    group by series_id, region
),

expected as (
    select series_id, region, explode(sequence(first_ts, last_ts, interval 1 hour)) as ts_utc
    from series
)

select e.series_id, e.region, e.ts_utc
from expected as e
left anti join {{ source("silver", "smard_timeseries") }} as s
    on s.series_id = e.series_id
    and s.region = e.region
    and s.resolution = 'hour'
    and s.ts_utc = e.ts_utc
-- Known gap in SMARD's offshore wind forecast: no values on 2015-01-04, local time.
where not (
    e.series_id = 3791
    and e.ts_utc >= timestamp'2015-01-03T23:00:00Z'
    and e.ts_utc < timestamp'2015-01-04T23:00:00Z'
)
