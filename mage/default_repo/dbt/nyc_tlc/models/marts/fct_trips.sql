{{ config(materialized='table') }}

with src as (
  -- Trae todos los campos necesarios desde SILVER
  select
      vendor_id,
      pickup_datetime,
      dropoff_datetime,
      passenger_count,
      trip_distance_clean  as trip_distance,
      total_amount_clean   as total_amount,
      try_cast(tip_amount as float) as tip_amount,
      trip_minutes,
      payment_type,
      ratecode_id,
      pu_location_id,
      do_location_id,
      service_type,
      year,
      month,
      run_id,
      ingest_ts
  from {{ source('silver', 'silver_trips') }}
  -- Filtro opcional por fechas:
  -- where pickup_datetime >= '2019-01-01' and pickup_datetime < '2020-01-01'
),

-- Dimensiones (GOLD)
zones as (
  select zone_sk, zone_id from {{ ref('dim_zone') }}
),
pay as (
  select payment_type_sk, payment_type from {{ ref('dim_payment_type') }}
),
rate as (
  select ratecode_sk, ratecode_id from {{ ref('dim_ratecode') }}
),

-- Mapeo de claves naturales -> SKs de dimensiones
mapped as (
  select
    s.vendor_id,
    s.pickup_datetime,
    s.dropoff_datetime,
    s.passenger_count,
    s.trip_distance,
    s.total_amount,
    s.tip_amount,  
    s.trip_minutes,
    s.service_type,
    s.year,
    s.month,
    p.payment_type_sk,
    r.ratecode_sk,
    zpu.zone_sk as pu_zone_sk,
    zdo.zone_sk as do_zone_sk,
    s.run_id,
    s.ingest_ts
  from src s
  left join zones zpu on zpu.zone_id = s.pu_location_id
  left join zones zdo on zdo.zone_id = s.do_location_id
  left join pay   p   on p.payment_type = s.payment_type
  left join rate  r   on r.ratecode_id  = s.ratecode_id
),

-- Deduplicación
dedup as (
  select
    m.*,
    row_number() over (
      partition by
        m.service_type, m.vendor_id,
        m.pickup_datetime, m.dropoff_datetime,
        m.pu_zone_sk, m.do_zone_sk,
        m.payment_type_sk, m.ratecode_sk,
        m.trip_distance, m.total_amount
      order by m.ingest_ts desc, m.run_id desc
    ) as rn
  from mapped m
)

select
    -- SK consistente
    {{ dbt_utils.generate_surrogate_key([
      'service_type','vendor_id',
      'pickup_datetime','dropoff_datetime',
      'pu_zone_sk','do_zone_sk',
      'payment_type_sk','ratecode_sk',
      'trip_distance','total_amount'
    ]) }} as trip_sk,

    pu_zone_sk,
    do_zone_sk,
    payment_type_sk,
    ratecode_sk,

    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    total_amount,
    tip_amount,
    trip_minutes,
    service_type,
    year,
    month
from dedup
qualify rn = 1
