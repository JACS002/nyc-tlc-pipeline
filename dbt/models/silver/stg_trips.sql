{{ config(materialized='incremental', unique_key='trip_unique_id') }}

with base as (
  -- yellow
  select
    'yellow' as service_type,
    t.vendorid::int as vendor_id,
    t.tpep_pickup_datetime as pickup_at,
    t.tpep_dropoff_datetime as dropoff_at,
    t.passenger_count::int as passenger_count,
    t.trip_distance::float as trip_distance_miles,
    t.pulocationid::int as PULocationID,
    t.dolocationid::int as DOLocationID,
    t.ratecodeid::int as rate_code_id,
    t.payment_type::int as payment_type_id,
    t.fare_amount::float as fare_amount,
    t.extra::float as extra,
    t.mta_tax::float as mta_tax,
    t.tip_amount::float as tip_amount,
    t.tolls_amount::float as tolls_amount,
    t.improvement_surcharge::float as improvement_surcharge,
    t.total_amount::float as total_amount,
    t.congestion_surcharge::float as congestion_surcharge,
    t.trip_type::int as trip_type_id,
    t.store_and_fwd_flag as store_and_fwd_flag,
    {{ dbt_utils.generate_surrogate_key(['t.vendorid','t.tpep_pickup_datetime','t.tpep_dropoff_datetime','t.pulocationid','t.dolocationid']) }} as trip_unique_id
  from {{ source('bronze','yellow_trips') }} t
  union all
  -- green
  select
    'green' as service_type,
    t.vendorid::int,
    t.lpep_pickup_datetime,
    t.lpep_dropoff_datetime,
    t.passenger_count::int,
    t.trip_distance::float,
    t.pulocationid::int,
    t.dolocationid::int,
    t.ratecodeid::int,
    t.payment_type::int,
    t.fare_amount::float,
    t.extra::float,
    t.mta_tax::float,
    t.tip_amount::float,
    t.tolls_amount::float,
    t.improvement_surcharge::float,
    t.total_amount::float,
    t.congestion_surcharge::float,
    t.trip_type::int,
    t.store_and_fwd_flag,
    {{ dbt_utils.generate_surrogate_key(['t.vendorid','t.lpep_pickup_datetime','t.lpep_dropoff_datetime','t.pulocationid','t.dolocationid']) }}
  from {{ source('bronze','green_trips') }} t
), cleaned as (
  select
    *,
    datediff('minute', pickup_at, dropoff_at) as trip_minutes,
    case when trip_distance_miles >= 0 and trip_minutes >= 0 then true else false end as is_nonnegative,
    case when payment_type_id in (1,2,3,4,5,6) then payment_type_id else null end as payment_type_id_norm
  from base
  where pickup_at is not null and dropoff_at is not null and pickup_at <= dropoff_at
)
select
  service_type, vendor_id, pickup_at, dropoff_at, passenger_count, trip_distance_miles,
  PULocationID, DOLocationID, rate_code_id,
  payment_type_id_norm as payment_type_id,
  fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, congestion_surcharge, total_amount,
  trip_type_id, store_and_fwd_flag,
  trip_minutes,
  case when trip_minutes > 0 then (trip_distance_miles / (trip_minutes/60.0)) end as avg_mph,
  trip_unique_id
from cleaned
where is_nonnegative = true

{% if is_incremental() %}
  and pickup_at > (select coalesce(max(pickup_at), '1900-01-01') from {{ this }})
{% endif %}
