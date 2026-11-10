-- Per region and German local day, from the hourly table.
{{ grid_totals("grid_hourly", "local_date", "local_date", hours_in_day("local_date")) }}
