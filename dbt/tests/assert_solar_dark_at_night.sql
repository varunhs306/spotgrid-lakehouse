-- Solar output between midnight and 03:00 Berlin time means a wrong series id or a shifted time
-- zone. SMARD reports some night-time noise (at most 116 MWh an hour in 2024-2026, against a
-- 58 GWh midday peak), so only real output fails.
select series_id, resolution, ts_utc, value
from {{ source("silver", "smard_timeseries") }}
where series_id in (4068, 125)
    and hour(from_utc_timestamp(ts_utc, 'Europe/Berlin')) < 3
    and value > 1000 * case when resolution = 'quarterhour' then 0.25 else 1 end
    -- Known error in SMARD's solar forecast: ~1.6 GWh on the night of 2015-08-01.
    and not (series_id = 125 and to_date(from_utc_timestamp(ts_utc, 'Europe/Berlin')) = '2015-08-01')
