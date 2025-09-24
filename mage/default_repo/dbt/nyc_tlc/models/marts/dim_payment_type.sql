{{ config(materialized='table') }}

-- Dimensión de Payment Type a partir del lookup
select
  {{ dbt_utils.generate_surrogate_key(['payment_type']) }} as payment_type_sk,
  cast(payment_type as int)     as payment_type,
  cast(description  as string)  as description
from {{ ref('payment_type_lookup') }}
