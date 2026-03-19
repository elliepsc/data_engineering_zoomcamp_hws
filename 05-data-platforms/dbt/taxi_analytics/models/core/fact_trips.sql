/*
 * Core Model: Fact Trips
 * 
 * Purpose: Create analytics-ready trip fact table
 * - Aggregate trip metrics
 * - Add business logic
 * - Optimize for querying
 */

{{ config(
    materialized='table',
    partition_by={
        'field': 'pickup_datetime',
        'data_type': 'timestamp',
        'granularity': 'month'
    },
    cluster_by=['pickup_location_id', 'dropoff_location_id'],
    tags=['core', 'fact']
) }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_green_taxi') }}
    WHERE is_suspicious = FALSE  -- Filter out suspicious records
),

enriched AS (
    SELECT
        -- Keys
        trip_id,
        
        -- Dimensions
        vendor_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        
        -- Date dimensions
        DATE(pickup_datetime) AS pickup_date,
        EXTRACT(YEAR FROM pickup_datetime) AS pickup_year,
        EXTRACT(MONTH FROM pickup_datetime) AS pickup_month,
        EXTRACT(DAY FROM pickup_datetime) AS pickup_day,
        EXTRACT(DAYOFWEEK FROM pickup_datetime) AS pickup_dayofweek,
        EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
        
        -- Trip metrics
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        
        -- Financial metrics
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        
        -- Calculated metrics
        ROUND(trip_distance / NULLIF(trip_duration_minutes, 0) * 60, 2) AS avg_speed_mph,
        ROUND(total_amount / NULLIF(trip_distance, 0), 2) AS revenue_per_mile,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS created_at
        
    FROM staging
)

SELECT * FROM enriched
