{{ config(
    materialized='view',
    tags=['analytics', 'examples']
) }}

/*
Payment Analysis Example
Demonstrates usage of reusable macros
*/

SELECT 
    service_type,
    {{ get_payment_type_description('payment_type') }} AS payment_method,
    {{ get_rate_code_description('rate_code_id') }} AS rate_type,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_revenue
FROM {{ ref('fct_trips') }}
WHERE pickup_datetime >= CURRENT_DATE - INTERVAL '{{ days_back(30) }}' DAY
GROUP BY service_type, payment_type, rate_code_id
ORDER BY trip_count DESC

/*
Run with different time ranges:
dbt run --select fct_payment_analysis --vars '{"days_back": 90}'
*/
