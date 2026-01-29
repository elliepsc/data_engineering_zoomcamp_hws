/*
Mart model: Trips Fact Table
Objective: Final analytics-ready table with calculated metrics
Layer: Marts (business-ready)
*/

{{
  config(
    materialized='table',
    tags=['marts', 'facts']
  )
}}

with trips as (
    select * from {{ ref('int_trips_unioned') }}
),

trips_with_metrics as (
    select
        -- Original columns
        *,
        
        -- Calculated metrics
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration_seconds,
        timestamp_diff(dropoff_datetime, pickup_datetime, minute) as trip_duration_minutes,
        
        -- Business logic: invalid trip if negative/zero duration
        case
            when timestamp_diff(dropoff_datetime, pickup_datetime, second) <= 0 then true
            when trip_distance <= 0 then true
            when fare_amount <= 0 then true
            else false
        end as is_invalid_trip,
        
        -- Date dimensions for aggregations
        date(pickup_datetime) as pickup_date,
        extract(year from pickup_datetime) as pickup_year,
        extract(month from pickup_datetime) as pickup_month,
        extract(dayofweek from pickup_datetime) as pickup_dayofweek,
        extract(hour from pickup_datetime) as pickup_hour

    from trips
)

select * from trips_with_metrics

/*
Notes:
- Materialization TABLE for performance (vs VIEW)
- Business metrics calculated once
- Temporal dimensions for future GROUP BY
- is_invalid_trip enables filtering in analyses
*/
