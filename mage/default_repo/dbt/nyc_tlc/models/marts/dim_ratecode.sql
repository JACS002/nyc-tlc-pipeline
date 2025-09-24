{{ config(materialized='table') }}

-- Dimensión de Ratecode a partir del lookup
select
  {{ dbt_utils.generate_surrogate_key(['ratecode_id']) }} as ratecode_sk,
  cast(ratecode_id as int)      as ratecode_id,
  cast(description as string)   as description
from {{ ref('ratecode_lookup') }}
