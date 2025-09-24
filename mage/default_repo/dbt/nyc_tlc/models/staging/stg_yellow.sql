{{ config(materialized='view') }}

with src as (
  select
      cast(vendorid as integer)               as vendor_id,
      cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
      cast(tpep_dropoff_datetime as timestamp)as dropoff_datetime,
      cast(passenger_count as integer)        as passenger_count,
      cast(trip_distance as float)            as trip_distance,
      cast(ratecodeid as integer)             as ratecode_id,
      cast(store_and_fwd_flag as string)      as store_and_fwd_flag,
      cast(pulocationid as integer)           as pu_location_id,
      cast(dolocationid as integer)           as do_location_id,
      cast(payment_type as integer)           as payment_type,
      cast(fare_amount as float)              as fare_amount,
      cast(extra as float)                    as extra,
      cast(mta_tax as float)                  as mta_tax,
      cast(tip_amount as float)               as tip_amount,
      cast(tolls_amount as float)             as tolls_amount,
      cast(improvement_surcharge as float)    as improvement_surcharge,
      cast(total_amount as float)             as total_amount,
      cast(congestion_surcharge as float)     as congestion_surcharge,
      cast(airport_fee as float)              as airport_fee,
      cast(cbd_congestion_fee as float)       as cbd_congestion_fee,
      null::integer                           as trip_type,
      'yellow'                                as service_type,
      cast(year as integer)                   as year,
      cast(month as integer)                  as month,
      cast(run_id as string)                  as run_id,
      try_to_timestamp_tz(ingest_ts)          as ingest_ts,
      source_url                              as source_url
  from {{ source('bronze','yellow_trips') }}
  where year is not null and month is not null
)
select * from src
