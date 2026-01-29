

{{
  config(
    materialized='view',
    tags=['staging', 'green_taxi']
  )
}}


{{
  config(
    materialized='view',
    tags=['staging', 'green_taxi']
  )
}}

with source as (
    -- DuckDB reads parquet files directly
    -- BigQuery will use {{ source('raw', 'green_tripdata') }} instead
    {% if target.type == 'duckdb' %}
        select * from read_parquet('data/raw/green_tripdata_*.parquet')
    {% else %}
        select * from {{ source('raw', 'green_tripdata') }}
    {% endif %}
),

renamed as (
    select
        -- Identifiers
        cast(vendorid as integer) as vendor_id,
        cast(ratecodeid as integer) as rate_code_id,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        
        -- Timestamps: consistent renaming across green/yellow
        cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
        
        -- Trip metrics
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as numeric) as trip_distance,
        
        -- Flags and codes
        cast(store_and_fwd_flag as string) as store_and_forward_flag,
        cast(payment_type as integer) as payment_type,
        
        -- Financial amounts
        cast(fare_amount as numeric) as fare_amount,
        cast(extra as numeric) as extra,
        cast(mta_tax as numeric) as mta_tax,
        cast(tip_amount as numeric) as tip_amount,
        cast(tolls_amount as numeric) as tolls_amount,
        cast(improvement_surcharge as numeric) as improvement_surcharge,
        cast(total_amount as numeric) as total_amount,
        cast(congestion_surcharge as numeric) as congestion_surcharge,
        
        -- Taxi type identifier (for union with yellow)
        'Green' as service_type

    from source
    
    -- Filter invalid data
    where vendorid is not null
      and lpep_pickup_datetime is not null
      and lpep_dropoff_datetime is not null
)

select * from renamed


