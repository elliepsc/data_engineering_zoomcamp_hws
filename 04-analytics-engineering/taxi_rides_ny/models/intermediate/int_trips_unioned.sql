/*
Intermediate model: Union of Green and Yellow trips
Objective: Consolidate both taxi types into single dataset
Layer: Intermediate (business logic)
*/

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

/*
dbt Notes:
- {{ ref('model') }} = reference to another dbt model
- dbt automatically manages dependencies (lineage)
- UNION ALL because identical structures guaranteed by staging
- This model is required for Q1 (understand dbt run --select)
*/
