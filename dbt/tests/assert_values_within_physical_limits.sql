-- Values the power system cannot produce, or the day-ahead market cannot clear. Catches
-- mislabelled series ids, unit mix-ups and shifted time zones; a series missing from the
-- catalog fails too. Volume limits are Germany's, per hour; a quarter-hour holds a quarter.
-- Kept in step with the quarantine in spotgrid_lakehouse.transforms.smard.
{%- set rows = [] %}
{%- for s in var("grid") if s.limits %}
{%- do rows.append("(" ~ s.id ~ ", 'DE', " ~ s.limits[0] ~ ", " ~ s.limits[1] ~ ", true)") %}
{%- endfor %}
{%- for p in var("prices") if p.limits %}
{%- do rows.append("(" ~ p.id ~ ", null, " ~ p.limits[0] ~ ", " ~ p.limits[1] ~ ", false)") %}
{%- endfor %}
{%- set ids = var("grid") | map(attribute="id") | list + var("prices") | map(attribute="id") | list %}

with limits as (
    select * from (values {{ rows | join(", ") }})
        as t(series_id, region, min_value, max_value, per_slot)
),

checked as (
    select
        s.series_id,
        s.region,
        s.resolution,
        s.ts_utc,
        s.value,
        s.series_id not in ({{ ids | join(", ") }}) as unknown_series,
        case when l.per_slot and s.resolution = 'quarterhour' then 0.25 else 1 end as scale,
        l.min_value,
        l.max_value
    from {{ source("silver", "smard_timeseries") }} as s
    left join limits as l
        on l.series_id = s.series_id and (l.region is null or l.region = s.region)
)

select *
from checked
where unknown_series
    or value < min_value * scale
    or value > max_value * scale
