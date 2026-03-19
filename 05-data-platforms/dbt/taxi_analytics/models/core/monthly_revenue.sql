/*
 * Core Model: Monthly Revenue Summary
 * 
 * Purpose: Monthly aggregated revenue metrics
 * - Total trips and revenue by month
 * - Average metrics
 * - Growth calculations
 */

{{ config(
    materialized='table',
    tags=['core', 'aggregate']
) }}

WITH trips AS (
    SELECT * FROM {{ ref('fact_trips') }}
),

monthly_metrics AS (
    SELECT
        pickup_year,
        pickup_month,
        
        -- Trip counts
        COUNT(*) AS total_trips,
        COUNT(DISTINCT pickup_location_id) AS unique_pickup_locations,
        COUNT(DISTINCT dropoff_location_id) AS unique_dropoff_locations,
        
        -- Passenger metrics
        SUM(passenger_count) AS total_passengers,
        ROUND(AVG(passenger_count), 2) AS avg_passengers_per_trip,
        
        -- Distance metrics
        ROUND(SUM(trip_distance), 2) AS total_distance_miles,
        ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
        
        -- Duration metrics
        ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes,
        
        -- Revenue metrics
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip,
        ROUND(SUM(tip_amount), 2) AS total_tips,
        ROUND(AVG(tip_amount), 2) AS avg_tip_per_trip,
        
        -- Calculated metrics
        ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph,
        ROUND(AVG(revenue_per_mile), 2) AS avg_revenue_per_mile
        
    FROM trips
    GROUP BY pickup_year, pickup_month
),

with_growth AS (
    SELECT
        *,
        
        -- Month-over-month growth
        LAG(total_revenue) OVER (ORDER BY pickup_year, pickup_month) AS prev_month_revenue,
        ROUND(
            (total_revenue - LAG(total_revenue) OVER (ORDER BY pickup_year, pickup_month)) 
            / NULLIF(LAG(total_revenue) OVER (ORDER BY pickup_year, pickup_month), 0) * 100,
            2
        ) AS revenue_growth_pct
        
    FROM monthly_metrics
)

SELECT * FROM with_growth
ORDER BY pickup_year, pickup_month
