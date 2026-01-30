{% macro timestamp_diff(unit, start_ts, end_ts) %}
    {%- if target.type == 'duckdb' -%}
        {%- if unit == 'second' -%}
            (epoch({{ end_ts }}) - epoch({{ start_ts }}))
        {%- else -%}
            date_diff('{{ unit }}', {{ start_ts }}, {{ end_ts }})
        {%- endif -%}
    {%- elif target.type == 'bigquery' -%}
        TIMESTAMP_DIFF({{ end_ts }}, {{ start_ts }}, {{ unit | upper }})
    {%- endif -%}
{% endmacro %}
