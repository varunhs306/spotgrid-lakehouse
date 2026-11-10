-- What silver holds per series, area and resolution: first and last slot, and last change.
select
    {{ series_column() }} as series,
    series_id,
    {{ area() }} as area,
    resolution,
    count(*) as slots,
    min(ts_utc) as first_ts_utc,
    max(ts_utc) as last_ts_utc,
    max(last_changed_at) as last_changed_at
from {{ source("silver", "smard_timeseries") }}
group by all
