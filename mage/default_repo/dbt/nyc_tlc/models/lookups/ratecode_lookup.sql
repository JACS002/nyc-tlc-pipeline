{{ config(
    materialized='table',
    schema='LOOKUPS'
) }}

with base as (
    select * from (
        values
            (1,  'Standard rate'),
            (2,  'JFK'),
            (3,  'Newark'),
            (4,  'Nassau or Westchester'),
            (5,  'Negotiated fare'),
            (6,  'Group ride')
    ) as t(ratecode_id, description)
),
extras as (
    -- códigos detectados en tus datos
    select 99,  'Other/Unspecified' as description
    union all
    select -1,  'Unknown/Null'
),
unioned as (
    select * from base
    union all
    select * from extras
)
select
    ratecode_id::int    as ratecode_id,
    description::string as description
from unioned
qualify row_number() over (partition by ratecode_id order by ratecode_id) = 1
