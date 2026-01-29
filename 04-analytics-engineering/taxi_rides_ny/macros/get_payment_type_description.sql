{% macro get_payment_type_description(column_name) %}
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
