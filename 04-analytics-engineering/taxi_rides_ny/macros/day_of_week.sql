{% macro day_of_week(ts) %}
    {%- if target.type == 'duckdb' -%}
        extract(dow from {{ ts }})
    {%- elif target.type == 'bigquery' -%}
        extract(dayofweek from {{ ts }})
    {%- endif -%}
{% endmacro %}
