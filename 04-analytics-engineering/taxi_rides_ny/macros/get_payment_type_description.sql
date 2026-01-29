{% macro get_payment_type_description(column_name) %}
/*
Macro: Payment Type Description
Purpose: Convert payment_type code to human-readable string
Usage: {{ get_payment_type_description('payment_type') }}

Example:
SELECT 
    {{ get_payment_type_description('payment_type') }} AS payment_method,
    COUNT(*) AS trip_count
FROM {{ ref('stg_green_tripdata') }}
GROUP BY 1
*/
    CASE {{ column_name }}
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Unknown'
    END
{% endmacro %}
