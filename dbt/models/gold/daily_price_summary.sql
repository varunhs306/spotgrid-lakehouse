-- Day-ahead price statistics per German local day.
select
    local_date,
    min(price_eur_mwh) as min_price_eur_mwh,
    round(avg(price_eur_mwh), 2) as avg_price_eur_mwh,
    max(price_eur_mwh) as max_price_eur_mwh,
    count_if(price_eur_mwh < 0) as negative_price_hours,
    count(*) as price_hours
from {{ ref("hourly_features") }}
where price_eur_mwh is not null
group by local_date
