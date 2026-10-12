-- Values the German power system cannot produce, or the day-ahead market cannot clear. Catches
-- mislabelled series ids, unit mix-ups and shifted time zones; a series with no limits fails too.
-- Volume limits are per hour, with headroom for capacity growth (2026: onshore ~65 GW, offshore
-- ~10 GW, solar ~115 GW). Observed 2024-2026: load 32.6-78.2 GWh, solar up to 58 GWh, prices
-- -499 to 936 EUR/MWh. A quarter-hour slot holds a quarter of an hour's energy.
with limits as (
    select * from values
        (4169, -500, 4000, false),     -- day-ahead price DE-LU, EUR/MWh: SDAC clearing limits
        (251, -500, 4000, false),      -- day-ahead price DE-AT-LU, until 2018-09-30
        (410, 20000, 100000, true),    -- grid load, MWh
        (4067, 0, 80000, true),        -- wind onshore
        (123, 0, 80000, true),         -- wind onshore forecast
        (1225, 0, 20000, true),        -- wind offshore
        (3791, 0, 20000, true),        -- wind offshore forecast
        (4068, 0, 150000, true),       -- solar
        (125, 0, 150000, true)         -- solar forecast
        as t(series_id, min_value, max_value, per_slot)
),

checked as (
    select
        s.series_id,
        s.resolution,
        s.ts_utc,
        s.value,
        l.series_id is null as unknown_series,
        case when l.per_slot and s.resolution = 'quarterhour' then 0.25 else 1 end as scale,
        l.min_value,
        l.max_value
    from {{ source("silver", "smard_timeseries") }} as s
    left join limits as l on s.series_id = l.series_id
)

select *
from checked
where unknown_series
    or value < min_value * scale
    or value > max_value * scale
