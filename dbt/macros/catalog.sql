{# Helpers over the series catalog passed in as vars. #}

{% macro grid_series(kinds=none) -%}
    {%- set out = [] -%}
    {%- for s in var("grid") if kinds is none or s.kind in kinds -%}
        {%- do out.append(s) -%}
    {%- endfor -%}
    {{ return(out) }}
{%- endmacro %}

{% macro series_by_column(column) -%}
    {%- for s in var("grid") if s.column == column -%}
        {{ return(s) }}
    {%- endfor -%}
    {{ return(none) }}
{%- endmacro %}

{# A component of a sum: zero where SMARD publishes no such series for the region, so totals
   stay defined; null where it should exist but is not published yet. #}
{% macro part(column) -%}
    {%- set s = series_by_column(column) -%}
    {%- if s is none -%}
        0
    {%- else -%}
        coalesce({{ column }}, case when region in ('{{ s.regions | join("', '") }}') then null else 0 end)
    {%- endif -%}
{%- endmacro %}

{# Slots of local days touched since this incremental model last ran. #}
{% macro changed_days(resolution, ids) -%}
    {%- if is_incremental() %}
    and local_date in (
        select distinct local_date
        from {{ source("silver", "smard_timeseries") }}
        where resolution = '{{ resolution }}'
            and series_id in ({{ ids | join(", ") }})
            and merged_at > (select coalesce(max(updated_at), timestamptz '1900-01-01') from {{ this }})
    )
    {%- endif %}
{%- endmacro %}

{# Hours in a German local day: 23, 24 or 25. #}
{% macro hours_in_day(day) -%}
    ((epoch(timezone('Europe/Berlin', ({{ day }} + interval 1 day)::timestamp))
      - epoch(timezone('Europe/Berlin', {{ day }}::timestamp))) / 3600)::integer
{%- endmacro %}

{# Name a silver row: the series column it fills, and the region or bidding zone it is for. #}
{% macro series_column(id_column="series_id") -%}
    case {{ id_column }}
        {%- for s in var("grid") %}
        when {{ s.id }} then '{{ s.column }}'
        {%- endfor %}
        {%- for p in var("prices") %}
        when {{ p.id }} then 'price_eur_mwh'
        {%- endfor %}
    end
{%- endmacro %}

{% macro area(id_column="series_id", region_column="region") -%}
    case {{ id_column }}
        {%- for p in var("prices") %}
        when {{ p.id }} then '{{ p.zone }}'
        {%- endfor %}
        else {{ region_column }}
    end
{%- endmacro %}
