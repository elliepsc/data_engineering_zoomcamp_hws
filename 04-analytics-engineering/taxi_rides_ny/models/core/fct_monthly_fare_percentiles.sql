{{ config(
    materialized='table',
    tags=['analytics', 'advanced', 'percentiles']
) }}

/*
Monthly Fare Percentiles Analysis
Objective: Calculate P90, P95, P97 fare distributions for pricing analysis
Business Value: Anomaly detection, pricing strategy, quality monitoring

Related to: Module 4 2025 Question 6 (Fare Percentiles)
*/

WITH monthly_fares AS (
    SELECT 
        service_type,
        DATE_TRUNC('month', pickup_datetime) AS month,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month_number,
        fare_amount,
        trip_distance,
        payment_type
    FROM {{ ref('fct_trips') }}
    WHERE fare_amount > 0
        AND trip_distance > 0
        AND is_invalid_trip = false
)

SELECT 
    service_type,
    year,
    month_number,
    month,
    
    -- Percentile calculations (DuckDB syntax)
    -- For BigQuery, use: PERCENTILE_CONT(fare_amount, 0.90) OVER()
    {% if target.type == 'duckdb' %}
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY fare_amount) AS p90_fare,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fare_amount) AS p95_fare,
        PERCENTILE_CONT(0.97) WITHIN GROUP (ORDER BY fare_amount) AS p97_fare,
    {% else %}
        PERCENTILE_CONT(fare_amount, 0.90) OVER() AS p90_fare,
        PERCENTILE_CONT(fare_amount, 0.95) OVER() AS p95_fare,
        PERCENTILE_CONT(fare_amount, 0.97) OVER() AS p97_fare,
    {% endif %}
    
    -- Supporting metrics
    COUNT(*) AS trip_count,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(MIN(fare_amount), 2) AS min_fare,
    ROUND(MAX(fare_amount), 2) AS max_fare,
    
    -- Distance metrics for context
    ROUND(AVG(trip_distance), 2) AS avg_distance
    
FROM monthly_fares
GROUP BY service_type, year, month_number, month
ORDER BY service_type, year, month_number

/*
Expected output (April 2020 from homework 2025):
- Green: {p97: 40.0, p95: 33.0, p90: 24.5}
- Yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

Query to verify:
SELECT service_type, p90_fare, p95_fare, p97_fare
FROM fct_monthly_fare_percentiles
WHERE year = 2020 AND month_number = 4;
*/
