{{ config(
    materialized='table',
    tags=['analytics', 'advanced', 'growth']
) }}

/*
Monthly Revenue Growth Analysis
Objective: Calculate MoM growth rates by zone and service type
Business Value: Performance tracking, trend analysis, strategic planning

Skills demonstrated:
- Window functions (LAG)
- Business metric calculations
- Financial analysis patterns
*/

WITH trips_with_zones AS (
    SELECT 
        t.*,
        pickup_zone.zone AS pickup_zone,
        pickup_zone.borough AS pickup_borough
    FROM {{ ref('fct_trips') }} t
    LEFT JOIN {{ ref('dim_zones') }} pickup_zone 
        ON t.pickup_location_id = pickup_zone.location_id
    WHERE t.is_invalid_trip = false
        AND pickup_zone.zone IS NOT NULL
),

monthly_revenue AS (
    SELECT 
        service_type,
        pickup_zone,
        pickup_borough,
        DATE_TRUNC('month', pickup_datetime) AS month,
        pickup_year AS year,
        pickup_month AS month_number,
        SUM(total_amount) AS revenue,
        COUNT(*) AS trip_count
    FROM trips_with_zones
    GROUP BY service_type, pickup_zone, pickup_borough, month, year, month_number
)

SELECT 
    service_type,
    pickup_zone,
    pickup_borough,
    year,
    month_number,
    month,
    ROUND(revenue, 2) AS revenue,
    trip_count,
    
    -- Previous month metrics using LAG window function
    ROUND(LAG(revenue, 1) OVER (
        PARTITION BY service_type, pickup_zone 
        ORDER BY year, month_number
    ), 2) AS prev_month_revenue,
    
    LAG(trip_count, 1) OVER (
        PARTITION BY service_type, pickup_zone 
        ORDER BY year, month_number
    ) AS prev_month_trip_count,
    
    -- Month-over-Month growth percentage
    ROUND(
        100.0 * (revenue - LAG(revenue, 1) OVER (
            PARTITION BY service_type, pickup_zone 
            ORDER BY year, month_number
        )) / NULLIF(LAG(revenue, 1) OVER (
            PARTITION BY service_type, pickup_zone 
            ORDER BY year, month_number
        ), 0),
        2
    ) AS mom_growth_pct,
    
    -- Trip count growth
    ROUND(
        100.0 * (trip_count - LAG(trip_count, 1) OVER (
            PARTITION BY service_type, pickup_zone 
            ORDER BY year, month_number
        )) / NULLIF(LAG(trip_count, 1) OVER (
            PARTITION BY service_type, pickup_zone 
            ORDER BY year, month_number
        ), 0),
        2
    ) AS mom_trip_growth_pct,
    
    -- Revenue per trip
    ROUND(revenue / NULLIF(trip_count, 0), 2) AS revenue_per_trip

FROM monthly_revenue
ORDER BY service_type, pickup_zone, year, month_number

/*
Sample queries:

-- Find zones with highest growth in January 2020
SELECT service_type, pickup_zone, revenue, mom_growth_pct
FROM fct_monthly_revenue_growth
WHERE year = 2020 AND month_number = 1
    AND mom_growth_pct IS NOT NULL
ORDER BY mom_growth_pct DESC
LIMIT 10;

-- Identify declining zones (negative growth > 2 months)
WITH declining_zones AS (
    SELECT 
        service_type, pickup_zone,
        COUNT(*) as negative_months
    FROM fct_monthly_revenue_growth
    WHERE mom_growth_pct < 0
    GROUP BY service_type, pickup_zone
    HAVING COUNT(*) > 2
)
SELECT * FROM declining_zones
ORDER BY negative_months DESC;
*/
