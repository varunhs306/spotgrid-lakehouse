-- SMARD's hourly price is the mean of its four quarter-hours and hourly volumes are their sum,
-- up to rounding. Checked on settled hours only: the two resolutions are revised separately.
-- Covers the German series the daily report watches.
{%- set prices = var("prices") | map(attribute="id") | list %}
{%- set watched = [4169, 251, 410, 4067, 1225, 4068, 123, 3791, 125] %}

with quarters as (
    select
        series_id,
        region,
        date_trunc('hour', ts_utc) as hour_utc,
        count(*) as quarters,
        case when series_id in ({{ prices | join(", ") }}) then avg(value) else sum(value) end
            as derived
    from {{ source("silver", "smard_timeseries") }}
    where resolution = 'quarterhour'
        and series_id in ({{ watched | join(", ") }})
        and region in ('DE', 'DE-LU', 'DE-AT-LU')
    group by series_id, region, date_trunc('hour', ts_utc)
)

select q.series_id, q.hour_utc, h.value as hourly, q.derived
from quarters as q
join {{ source("silver", "smard_timeseries") }} as h
    on h.series_id = q.series_id
    and h.region = q.region
    and h.ts_utc = q.hour_utc
    and h.resolution = 'hour'
where q.quarters = 4
    and q.hour_utc < now() - interval 2 days
    and abs(q.derived - h.value) > case when q.series_id in (4169, 251) then 0.01 else 0.05 end
