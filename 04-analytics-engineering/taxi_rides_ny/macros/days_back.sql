{% macro days_back(default=30) %}
/*
Macro: Days Back Configuration
Purpose: Centralize date range logic with environment variable support
Precedence: CLI var > ENV var > default value

Usage in model:
WHERE pickup_datetime >= CURRENT_DATE - INTERVAL '{{ days_back(7) }}' DAY

Command line examples:
dbt run --vars '{"days_back": 90}'  # Override with CLI
export DAYS_BACK=60 && dbt run      # Override with ENV var
dbt run                             # Use default (30)

Related to: Module 4 2025 Question 2 (dbt Variables & Dynamic Models)
*/
    {{ var("days_back", env_var("DAYS_BACK", default)) }}
{% endmacro %}
