-- DE-AT-LU (251) ends 2018-09-30 21:00 UTC and DE-LU (4169) starts the next hour: neither
-- crosses the boundary, and DE-LU has a price on both sides of it once history reaches it.
select 'series crosses the boundary' as problem, series_id, ts_utc
from {{ source("silver", "smard_timeseries") }}
where (series_id = 251 and ts_utc >= timestamptz '2018-09-30 22:00:00+00')
    or (series_id = 4169 and ts_utc < timestamptz '2018-09-30 22:00:00+00')

union all

select 'no price at the boundary', null, b.ts_utc
from (
    select unnest([timestamptz '2018-09-30 21:00:00+00', timestamptz '2018-09-30 22:00:00+00'])
        as ts_utc
) as b
where exists (
    select 1 from {{ ref("prices_hourly") }}
    where zone = 'DE-LU' and ts_utc < timestamptz '2018-09-30 21:00:00+00'
)
    and not exists (
        select 1 from {{ ref("prices_hourly") }} as h
        where h.zone = 'DE-LU' and h.ts_utc = b.ts_utc and h.price_eur_mwh is not null
    )
