{% macro days_back(default=30) %}
dbt run                             # Use default (30)

Related to: Module 4 2025 Question 2 (dbt Variables & Dynamic Models)
*/
    {{ var("days_back", env_var("DAYS_BACK", default)) }}
{% endmacro %}
