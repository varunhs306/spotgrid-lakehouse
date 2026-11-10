{{ config(materialized="incremental", incremental_strategy="delete+insert", unique_key=["zone", "ts_utc"]) }}
{{ price_slots("hour") }}
