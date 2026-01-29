

{{
  config(
    materialized='view',
    tags=['intermediate', 'trips']
  )
}}

with green_trips as (
    -- Reference to staging model (not raw source)
    select * from {{ ref('stg_green_tripdata') }}
),

yellow_trips as (
    select * from {{ ref('stg_yellow_tripdata') }}
),

trips_unioned as (
    -- Union of both taxi types
    select * from green_trips
    union all
    select * from yellow_trips
)

select * from trips_unioned

