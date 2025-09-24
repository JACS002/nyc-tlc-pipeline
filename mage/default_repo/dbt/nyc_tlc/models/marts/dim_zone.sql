{{ config(materialized='table') }}

with src as (
  select
    cast(locationid as int)   as zone_id,
    cast(borough    as string) as borough,
    cast(zone       as string) as zone
  from {{ source('bronze', 'taxi_zones') }}
)

select distinct
  {{ dbt_utils.generate_surrogate_key(['zone_id']) }} as zone_sk,
  zone_id,
  borough,
  zone
from src
