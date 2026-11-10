-- Per region and German local month, from the hourly table.
{{ grid_totals(
    "grid_hourly",
    "month",
    "date_trunc('month', local_date)::date",
    "((epoch(timezone('Europe/Berlin', (date_trunc('month', local_date) + interval 1 month)::timestamp))"
    ~ " - epoch(timezone('Europe/Berlin', date_trunc('month', local_date)::timestamp))) / 3600)::integer"
) }}
