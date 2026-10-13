-- One row per UTC hour with every SMARD series side by side. Forecast columns run a day
-- ahead of the actuals, so the newest rows have forecasts and prices but no load yet.
-- Prices: DE-AT-LU (251) until the zone split on 2018-10-01, DE-LU (4169) after.
{%- set series = {
    "price_eur_mwh": [4169, 251],
    "load_mwh": [410],
    "wind_onshore_mwh": [4067],
    "wind_offshore_mwh": [1225],
    "solar_mwh": [4068],
    "forecast_wind_onshore_mwh": [123],
    "forecast_wind_offshore_mwh": [3791],
    "forecast_solar_mwh": [125],
} %}

with pivoted as (
    select
        ts_utc,
        any_value(local_date) as local_date,
        max(region) filter (where series_id in (4169, 251)) as price_zone,
        {%- for column, ids in series.items() %}
        max(value) filter (where series_id in ({{ ids | join(", ") }})) as {{ column }}{{ "," if not loop.last }}
        {%- endfor %}
    from {{ source("silver", "smard_timeseries") }}
    where resolution = 'hour'
    group by ts_utc
)

select
    ts_utc,
    local_date,
    price_zone,
    {%- for column in series %}
    {{ column }},
    {%- endfor %}
    load_mwh - wind_onshore_mwh - wind_offshore_mwh - solar_mwh as residual_load_mwh,
    'hour' as source_resolution
from pivoted
