-- One row per area and slot in every slot table.
{%- set tables = [
    ("grid_quarterhour", "region", "ts_utc"),
    ("grid_hourly", "region", "ts_utc"),
    ("grid_daily", "region", "local_date"),
    ("grid_monthly", "region", "month"),
    ("prices_quarterhour", "zone", "ts_utc"),
    ("prices_hourly", "zone", "ts_utc"),
    ("prices_daily", "zone", "local_date"),
    ("prices_monthly", "zone", "month"),
] %}
{%- for name, area, slot in tables %}
select '{{ name }}' as model, {{ area }} as area, {{ slot }}::varchar as slot, count(*) as n
from {{ ref(name) }}
group by all
having count(*) > 1
{% if not loop.last %}union all{% endif %}
{%- endfor %}
