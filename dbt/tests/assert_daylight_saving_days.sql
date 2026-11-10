-- Spring-forward days have 23 hours and fall-back days 25, in hourly and 15-minute DE-LU prices.
with days as (
    select local_date, price_hours as slots, 1 as per_hour
    from {{ ref("prices_daily") }}
    where zone = 'DE-LU'
    union all
    select local_date, count(*), 4
    from {{ ref("prices_quarterhour") }}
    where zone = 'DE-LU' and source_resolution = 'quarterhour'
    group by local_date
),

expected as (
    select *, {{ hours_in_day("local_date") }} as hours
    from days
)

select * from expected
where hours <> 24 and slots <> hours * per_hour
