{{ config(materialized='view') }}

with unioned as (
  select * from {{ ref('stg_yellow') }}
  union all
  select * from {{ ref('stg_green') }}
),

filtered as (
  select *
  from unioned
  where pickup_datetime between '2009-01-01' and '2025-12-31'
),

clean as (
  select
    *,
    -- reglas mínimas de calidad en silver
    case when trip_distance < 0    then null else trip_distance end as trip_distance_clean,
    case when total_amount   < -50 then null else total_amount   end as total_amount_clean,
    datediff('minute', pickup_datetime, dropoff_datetime)          as trip_minutes
  from filtered
),

mapped as (
  select
    c.*,
    pt.description as payment_type_desc,
    rc.description as ratecode_desc
  from clean c
  left join {{ ref('payment_type_lookup') }} pt
    on pt.payment_type = c.payment_type
  left join {{ ref('ratecode_lookup') }} rc
    on rc.ratecode_id = c.ratecode_id
),

with_zones as (
  select
    m.*,
    tzp.borough as pu_borough,
    tzp.zone    as pu_zone,
    tzd.borough as do_borough,
    tzd.zone    as do_zone
  from mapped m
  left join {{ source('bronze','taxi_zones') }} tzp on tzp.locationid = m.pu_location_id
  left join {{ source('bronze','taxi_zones') }} tzd on tzd.locationid = m.do_location_id
)

select * from with_zones
