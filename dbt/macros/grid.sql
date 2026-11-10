{# One row per region and slot with every grid series side by side, plus derived features.
   A series SMARD does not publish for a region is null; parts of sums treat it as zero. #}
{% macro grid_slots(resolution) %}
{%- set grid = var("grid") %}
{%- set ids = grid | map(attribute="id") | list %}
{%- set actuals = grid_series(["generation", "consumption"]) %}

with slots as (
    select
        region,
        ts_utc,
        any_value(local_date) as local_date,
        {%- for s in grid %}
        max(value) filter (where series_id = {{ s.id }}) as {{ s.column }},
        {%- endfor %}
        max(merged_at) as updated_at
    from {{ source("silver", "smard_timeseries") }}
    where resolution = '{{ resolution }}'
        and series_id in ({{ ids | join(", ") }})
        {{- changed_days(resolution, ids) }}
    group by region, ts_utc
),

valued as (
    select
        region,
        ts_utc,
        local_date,
        {%- for s in grid %}
        {%- if s.ended %}
        case
            when local_date >= date '{{ s.ended }}' and region in ('{{ s.regions | join("', '") }}')
                then coalesce({{ s.column }}, 0)
            else {{ s.column }}
        end as {{ s.column }},
        {%- else %}
        {{ s.column }},
        {%- endif %}
        {%- endfor %}
        updated_at
    from slots
)

select
    region,
    ts_utc,
    local_date,
    {%- for s in grid %}
    {{ s.column }},
    {%- endfor %}
    wind_onshore_mwh - forecast_wind_onshore_mwh as wind_onshore_forecast_error_mwh,
    wind_offshore_mwh - forecast_wind_offshore_mwh as wind_offshore_forecast_error_mwh,
    solar_mwh - forecast_solar_mwh as solar_forecast_error_mwh,
    {{ renewable() }} as renewable_mwh,
    round(({{ renewable() }}) / nullif(load_mwh, 0) * 100, 2) as renewable_share_pct,
    {{ part("pumped_storage_mwh") }} - {{ part("pumped_storage_consumption_mwh") }}
        as net_pumped_storage_mwh,
    -- Every actual SMARD publishes for the region is in; forecasts run ahead and do not count.
    case region
        {%- for region in var("regions") %}
        when '{{ region }}' then
            {%- set present = [] %}
            {%- for s in actuals if region in s.regions %}
            {%- do present.append(s.column ~ " is not null") %}
            {%- endfor %}
            {{ present | join(" and ") if present else "false" }}
        {%- endfor %}
        else false
    end as is_complete,
    updated_at
from valued
{% endmacro %}

{% macro renewable() -%}
    {{ part("wind_onshore_mwh") }} + {{ part("wind_offshore_mwh") }} + {{ part("solar_mwh") }}
    + {{ part("hydro_mwh") }} + {{ part("biomass_mwh") }} + {{ part("other_renewable_mwh") }}
{%- endmacro %}

{# Energy summed over local days or months; shares recomputed from the sums. #}
{% macro grid_totals(source_model, period, period_expr, expected_hours) %}
{%- set grid = var("grid") %}
select
    region,
    {{ period_expr }} as {{ period }},
    count(*) as hours,
    {%- for s in grid %}
    sum({{ s.column }}) as {{ s.column }},
    {%- endfor %}
    sum(wind_onshore_forecast_error_mwh) as wind_onshore_forecast_error_mwh,
    sum(wind_offshore_forecast_error_mwh) as wind_offshore_forecast_error_mwh,
    sum(solar_forecast_error_mwh) as solar_forecast_error_mwh,
    sum(renewable_mwh) as renewable_mwh,
    round(sum(renewable_mwh) / nullif(sum(load_mwh), 0) * 100, 2) as renewable_share_pct,
    sum(net_pumped_storage_mwh) as net_pumped_storage_mwh,
    bool_and(is_complete) and count(*) = min({{ expected_hours }}) as is_complete,
    max(updated_at) as updated_at
from {{ ref(source_model) }}
group by all
{% endmacro %}
