

{{
  config(
    materialized='view',
    tags=['staging', 'fhv']
  )
}}

with source as (
    -- DuckDB reads parquet files directly
    {% if target.type == 'duckdb' %}
        select * from read_parquet('data/raw/fhv_tripdata_*.parquet')
    {% else %}
        select * from {{ source('raw', 'fhv_tripdata') }}
    {% endif %}
),

filtered_and_renamed as (
    select
        -- Dispatch base identifier (FHV business key)
        cast(dispatching_base_num as string) as dispatching_base_num,

        -- Timestamps
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        -- Locations
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- Shared ride flag
        cast(sr_flag as integer) as shared_ride_flag,

        -- Service type
        'FHV' as service_type

    from source

    -- REQUIRED Q6: Filter out NULL dispatching_base_num
    --where dispatching_base_num is not null
    --  and pickup_datetime is not null
    where pickup_datetime is not null
)

select * from filtered_and_renamed

