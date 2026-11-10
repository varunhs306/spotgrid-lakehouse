-- Every change SMARD made to a published value, with what it changed from and to. Delay runs
-- from the slot's start to the landing that carried the new value.
select
    {{ series_column() }} as series,
    series_id,
    {{ area() }} as area,
    resolution,
    ts_utc,
    local_date,
    old_value,
    new_value,
    round(new_value - old_value, 3) as change,
    detected_at,
    round((epoch(detected_at) - epoch(ts_utc)) / 86400, 2) as delay_days
from {{ source("silver", "revisions") }}
