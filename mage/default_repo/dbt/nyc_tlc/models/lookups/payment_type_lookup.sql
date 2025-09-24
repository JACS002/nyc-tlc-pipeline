{{ config(
    materialized='table',
    schema='LOOKUPS'
) }}

with base as (
    select * from (
        values
            (1, 'Credit Card'),
            (2, 'Cash'),
            (3, 'No Charge'),
            (4, 'Dispute'),
            (5, 'Unknown'),
            (6, 'Voided Trip')
    ) as t(payment_type, description)
),
extras as (
    -- valores que aparecían en tus datos
    select 0,  'Unspecified'
    union all
    select -1, 'Unknown/Null'
),
unioned as (
    select * from base
    union all
    select * from extras
)
select
    payment_type::int   as payment_type,
    description::string as description
from unioned
qualify row_number() over (partition by payment_type order by payment_type) = 1
