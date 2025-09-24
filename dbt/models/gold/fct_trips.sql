{{ config(materialized='table', cluster_by=['pickup_date_sk','PULocationID','service_type']) }}

with enriched as (
  select t.*,
         to_date(pickup_at)  as pickup_date,
         to_date(dropoff_at) as dropoff_date
  from {{ ref('stg_trips') }} t
)
select
  {{ dbt_utils.generate_surrogate_key(['trip_unique_id']) }} as trip_sk,
  pickup_date  as pickup_date_sk,
  dropoff_date as dropoff_date_sk,
  PULocationID, DOLocationID,
  service_type, vendor_id, rate_code_id, payment_type_id,
  passenger_count, trip_distance_miles, trip_minutes, avg_mph,
  fare_amount, tip_amount, total_amount, tolls_amount, congestion_surcharge, improvement_surcharge, extra, mta_tax,
  trip_type_id, store_and_fwd_flag
from enriched
