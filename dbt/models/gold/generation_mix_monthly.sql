-- Energy by source and the share of load met by wind and solar, per German local month. Only
-- hours with load and all three generation series count, so the current month is not
-- understated while its actuals are still being published.
with hours as (
    select *, trunc(local_date, 'MM') as month
    from {{ ref("hourly_features") }}
    where load_mwh is not null
        and wind_onshore_mwh is not null
        and wind_offshore_mwh is not null
        and solar_mwh is not null
)

select
    month,
    count(*) as hours,
    -- 743 or 745 hours in the months daylight saving starts or ends.
    count(*) = (
        unix_timestamp(to_utc_timestamp(add_months(month, 1), 'Europe/Berlin'))
        - unix_timestamp(to_utc_timestamp(month, 'Europe/Berlin'))
    ) / 3600 as is_complete,
    round(sum(load_mwh) / 1000, 1) as load_gwh,
    round(sum(wind_onshore_mwh) / 1000, 1) as wind_onshore_gwh,
    round(sum(wind_offshore_mwh) / 1000, 1) as wind_offshore_gwh,
    round(sum(solar_mwh) / 1000, 1) as solar_gwh,
    round(
        sum(wind_onshore_mwh + wind_offshore_mwh + solar_mwh) / sum(load_mwh) * 100, 1
    ) as wind_solar_share_pct,
    round(avg(price_eur_mwh), 2) as avg_price_eur_mwh,
    count_if(price_eur_mwh < 0) as negative_price_hours
from hours
group by month
