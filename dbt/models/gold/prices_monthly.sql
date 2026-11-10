-- Per zone and German local month, from the hourly prices.
{{ price_totals("month", "date_trunc('month', local_date)::date") }}
