/*
 * Staging Model: Green Taxi Trips
 * 
 * Purpose: Clean and standardize raw green taxi data
 * - Remove invalid records
 * - Add calculated fields
 * - Standardize datetime formats
 */

{{ config(
    materialized='view',
    tags=['staging', 'taxi']
) }}

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'green_taxi') }}
),

cleaned AS (
    SELECT
        -- Primary keys
        {{ dbt_utils.generate_surrogate_key(['VendorID', 'lpep_pickup_datetime', 'PULocationID']) }} AS trip_id,
        
        -- Identifiers
        VendorID AS vendor_id,
        
        -- Timestamps
        CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
        
        -- Trip info
        CAST(passenger_count AS INT64) AS passenger_count,
        CAST(trip_distance AS FLOAT64) AS trip_distance,
        CAST(PULocationID AS INT64) AS pickup_location_id,
        CAST(DOLocationID AS INT64) AS dropoff_location_id,
        
        -- Payment info
        CAST(fare_amount AS FLOAT64) AS fare_amount,
        CAST(extra AS FLOAT64) AS extra,
        CAST(mta_tax AS FLOAT64) AS mta_tax,
        CAST(tip_amount AS FLOAT64) AS tip_amount,
        CAST(tolls_amount AS FLOAT64) AS tolls_amount,
        CAST(improvement_surcharge AS FLOAT64) AS improvement_surcharge,
        CAST(total_amount AS FLOAT64) AS total_amount,
        
        -- Calculated fields
        TIMESTAMP_DIFF(
            CAST(lpep_dropoff_datetime AS TIMESTAMP),
            CAST(lpep_pickup_datetime AS TIMESTAMP),
            MINUTE
        ) AS trip_duration_minutes,
        
        -- Data quality flags
        CASE
            WHEN passenger_count < 1 THEN TRUE
            WHEN trip_distance <= 0 THEN TRUE
            WHEN total_amount <= 0 THEN TRUE
            ELSE FALSE
        END AS is_suspicious
        
    FROM source
    
    WHERE
        -- Basic data quality filters
        lpep_pickup_datetime IS NOT NULL
        AND lpep_dropoff_datetime IS NOT NULL
        AND lpep_pickup_datetime < lpep_dropoff_datetime
        AND trip_distance > 0
        AND fare_amount > 0
)

SELECT * FROM cleaned
