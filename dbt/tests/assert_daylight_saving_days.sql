-- Spring-forward days have 23 hours and fall-back days 25, in hourly and quarter-hour prices.
with days as (
    select local_date, price_hours as slots, 1 as per_hour
    from {{ ref("daily_price_summary") }}
    union all
    select local_date, count(price_eur_mwh), 4
    from {{ ref("quarterhour_features") }}
    group by local_date
),

expected as (
    select
        *,
        (unix_timestamp(to_utc_timestamp(date_add(local_date, 1), 'Europe/Berlin'))
            - unix_timestamp(to_utc_timestamp(local_date, 'Europe/Berlin'))) / 3600 as hours
    from days
)

select * from expected
where hours <> 24 and slots <> hours * per_hour
