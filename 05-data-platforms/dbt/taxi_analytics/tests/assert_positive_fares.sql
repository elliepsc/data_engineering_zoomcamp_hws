/*
 * Custom dbt Test: Assert Positive Fares
 * 
 * Ensures all fares in fact_trips are positive
 * Fails if any record has fare_amount <= 0
 */

SELECT
    trip_id,
    fare_amount
FROM {{ ref('fact_trips') }}
WHERE fare_amount <= 0
