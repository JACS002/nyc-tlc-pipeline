{{ config(materialized='view') }}
select * from {{ source('bronze', 'yellow_trips') }}
