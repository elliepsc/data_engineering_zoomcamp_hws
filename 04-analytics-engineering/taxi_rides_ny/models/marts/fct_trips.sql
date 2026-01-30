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
        *,
        
        {{ timestamp_diff('second', 'pickup_datetime', 'dropoff_datetime') }} as trip_duration_seconds,
        {{ timestamp_diff('minute', 'pickup_datetime', 'dropoff_datetime') }} as trip_duration_minutes,
        
        case
            when {{ timestamp_diff('second', 'pickup_datetime', 'dropoff_datetime') }} <= 0 then true
            when trip_distance <= 0 then true
            when fare_amount <= 0 then true
            else false
        end as is_invalid_trip,
        
        date(pickup_datetime) as pickup_date,
        extract(year from pickup_datetime) as pickup_year,
        extract(month from pickup_datetime) as pickup_month,
        {{ day_of_week('pickup_datetime') }} as pickup_dayofweek,
        extract(hour from pickup_datetime) as pickup_hour

    from trips
)

select * from trips_with_metrics

