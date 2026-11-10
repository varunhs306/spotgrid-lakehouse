{{ config(materialized="incremental", incremental_strategy="delete+insert", unique_key=["region", "ts_utc"]) }}
{{ grid_slots("quarterhour") }}
