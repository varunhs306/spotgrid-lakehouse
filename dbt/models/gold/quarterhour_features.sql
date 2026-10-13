-- One row per UTC quarter-hour since the day-ahead market moved to 15-minute products on
-- 2025-10-01. Before that, SMARD's quarter-hour prices repeat the hourly price, so they start here.
{%- set series = {
    "price_eur_mwh": 4169,
    "load_mwh": 410,
    "wind_onshore_mwh": 4067,
    "wind_offshore_mwh": 1225,
    "solar_mwh": 4068,
    "forecast_wind_onshore_mwh": 123,
    "forecast_wind_offshore_mwh": 3791,
    "forecast_solar_mwh": 125,
} %}

select
    ts_utc,
    any_value(local_date) as local_date,
    {%- for column, series_id in series.items() %}
    max(value) filter (where series_id = {{ series_id }}) as {{ column }}{{ "," if not loop.last }}
    {%- endfor %}
from {{ source("silver", "smard_timeseries") }}
where resolution = 'quarterhour' and ts_utc >= timestamp'2025-09-30T22:00:00Z'
group by ts_utc
