-- SMARD's residual load (4359) is grid load minus wind and solar generation. Germany's hours
-- with all five published should agree within 1 MWh; a gap means a series changed meaning.
{{ config(severity="warn") }}

select
    ts_utc,
    residual_load_mwh,
    load_mwh - wind_onshore_mwh - wind_offshore_mwh - solar_mwh as computed
from {{ ref("grid_hourly") }}
where region = 'DE'
    and abs(residual_load_mwh - (load_mwh - wind_onshore_mwh - wind_offshore_mwh - solar_mwh)) > 1
