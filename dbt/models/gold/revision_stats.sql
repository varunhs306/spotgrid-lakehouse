-- How often, how late and by how much SMARD revises each series, from the revisions log.
-- Delay runs from the slot's start to the landing that carried the new value, so it is only
-- as precise as the re-landing: daily for the last two weeks, weekly for the last year.
{%- set names = {
    4169: "PRICE_DE_LU",
    251: "PRICE_DE_AT_LU",
    410: "LOAD",
    4067: "WIND_ONSHORE",
    1225: "WIND_OFFSHORE",
    4068: "SOLAR",
    123: "FORECAST_WIND_ONSHORE",
    3791: "FORECAST_WIND_OFFSHORE",
    125: "FORECAST_SOLAR",
} %}

with slots as (
    select series_id, region, resolution, count(*) as slots, min(first_seen_at) as observed_since
    from {{ source("silver", "smard_timeseries") }}
    group by series_id, region, resolution
),

revisions as (
    select
        series_id,
        region,
        resolution,
        ts_utc,
        detected_at,
        (unix_timestamp(detected_at) - unix_timestamp(ts_utc)) / 86400 as delay_days,
        abs(new_value - old_value) as abs_change,
        abs(new_value - old_value) / nullif(abs(old_value), 0) * 100 as change_pct
    from {{ source("silver", "revisions") }}
)

select
    s.series_id,
    case s.series_id
        {%- for id, name in names.items() %}
        when {{ id }} then '{{ name }}'
        {%- endfor %}
    end as series_name,
    s.region,
    s.resolution,
    s.slots,
    count(r.ts_utc) as revisions,
    count(distinct r.ts_utc) as revised_slots,
    round(count(distinct r.ts_utc) / s.slots * 100, 3) as revised_slots_pct,
    round(percentile(r.delay_days, 0.5), 1) as median_delay_days,
    round(percentile(r.delay_days, 0.95), 1) as p95_delay_days,
    round(max(r.delay_days), 1) as max_delay_days,
    round(percentile(r.abs_change, 0.5), 2) as median_abs_change,
    round(percentile(r.abs_change, 0.95), 2) as p95_abs_change,
    round(max(r.abs_change), 2) as max_abs_change,
    round(percentile(r.change_pct, 0.5), 2) as median_change_pct,
    s.observed_since,
    max(r.detected_at) as last_revision_at
from slots as s
left join revisions as r
    on r.series_id = s.series_id and r.region = s.region and r.resolution = s.resolution
group by s.series_id, s.region, s.resolution, s.slots, s.observed_since
