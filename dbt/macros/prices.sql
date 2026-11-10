{# One row per bidding zone and slot, with the spread to DE-LU. DE-AT-LU (until 2018-09-30)
   is served as DE-LU; source_zone keeps the zone that cleared the price. #}
{% macro price_slots(resolution) %}
{%- set prices = var("prices") %}
{%- set ids = prices | map(attribute="id") | list %}

with zoned as (
    select
        case series_id
            {%- for p in prices %}
            when {{ p.id }} then '{{ p.zone }}'
            {%- endfor %}
        end as zone,
        case series_id
            {%- for p in prices %}
            when {{ p.id }} then '{{ p.source_zone }}'
            {%- endfor %}
        end as source_zone,
        ts_utc,
        local_date,
        value as price_eur_mwh,
        merged_at as updated_at
    from {{ source("silver", "smard_timeseries") }}
    where resolution = '{{ resolution }}'
        and series_id in ({{ ids | join(", ") }})
        {{- changed_days(resolution, ids) }}
)

select
    z.zone,
    z.ts_utc,
    z.local_date,
    z.price_eur_mwh,
    round(z.price_eur_mwh - d.price_eur_mwh, 2) as spread_to_de_lu_eur_mwh,
    z.source_zone,
    {%- if resolution == "quarterhour" %}
    -- Day-ahead products became 15 minutes long on 2025-10-01; before that SMARD's
    -- quarter-hours repeat the hourly price.
    case when z.ts_utc < timestamptz '2025-09-30 22:00:00+00' then 'hour' else 'quarterhour' end
    {%- else %}
    'hour'
    {%- endif %} as source_resolution,
    z.updated_at
from zoned as z
left join zoned as d on d.zone = 'DE-LU' and d.ts_utc = z.ts_utc
{% endmacro %}

{# Base, peak and off-peak per zone and period. Peak is 08:00-20:00 local, Monday to Friday. #}
{% macro price_totals(period, period_expr) %}
with hours as (
    select
        *,
        isodow(local_date) <= 5
            and hour(timezone('Europe/Berlin', ts_utc)) between 8 and 19 as is_peak
    from {{ ref("prices_hourly") }}
)

select
    zone,
    {{ period_expr }} as {{ period }},
    count(*) as price_hours,
    round(avg(price_eur_mwh), 2) as base_eur_mwh,
    round(avg(price_eur_mwh) filter (where is_peak), 2) as peak_eur_mwh,
    round(avg(price_eur_mwh) filter (where not is_peak), 2) as off_peak_eur_mwh,
    min(price_eur_mwh) as min_eur_mwh,
    max(price_eur_mwh) as max_eur_mwh,
    max(price_eur_mwh) - min(price_eur_mwh) as range_eur_mwh,
    count(*) filter (where price_eur_mwh < 0) as negative_hours,
    round(avg(spread_to_de_lu_eur_mwh), 2) as avg_spread_to_de_lu_eur_mwh,
    max(updated_at) as updated_at
from hours
group by all
{% endmacro %}
