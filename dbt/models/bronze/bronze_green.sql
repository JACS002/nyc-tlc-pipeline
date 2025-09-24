{{ config(materialized='view') }}
select * from {{ source('bronze', 'green_trips') }}
