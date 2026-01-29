{% macro get_rate_code_description(column_name) %}
/*
Macro: Rate Code Description
Purpose: Convert rate_code_id to human-readable string
Usage: {{ get_rate_code_description('rate_code_id') }}

Example:
SELECT 
    {{ get_rate_code_description('rate_code_id') }} AS rate_type,
    COUNT(*) AS trip_count
FROM {{ ref('fct_trips') }}
GROUP BY 1
*/
    CASE {{ column_name }}
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        ELSE 'Unknown'
    END
{% endmacro %}
