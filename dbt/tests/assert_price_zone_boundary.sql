-- DE-AT-LU (251) ends 2018-09-30 21:00 UTC and DE-LU (4169) starts the next hour: neither
-- crosses the boundary, and gold has a price on both sides of it.
select 'series crosses the boundary' as problem, series_id, ts_utc
from {{ source("silver", "smard_timeseries") }}
where (series_id = 251 and ts_utc >= timestamp'2018-09-30T22:00:00Z')
    or (series_id = 4169 and ts_utc < timestamp'2018-09-30T22:00:00Z')

union all

select 'no price at the boundary', null, b.ts_utc
from (
    select explode(array(timestamp'2018-09-30T21:00:00Z', timestamp'2018-09-30T22:00:00Z'))
        as ts_utc
) as b
left anti join {{ ref("hourly_features") }} as h
    on h.ts_utc = b.ts_utc and h.price_eur_mwh is not null
