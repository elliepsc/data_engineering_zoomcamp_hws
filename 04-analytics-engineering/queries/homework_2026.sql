-- ============================================================
-- Module 4 Homework - 2026 Version
-- SQL Queries for all 6 homework questions
-- ============================================================
-- 
-- Usage:
--   Option 1: Copy-paste into DuckDB CLI
--   Option 2: Run entire file: duckdb taxi_rides_ny.duckdb < homework_2026.sql
--   Option 3: Run in container: docker-compose exec dbt duckdb taxi_rides_ny.duckdb < homework_2026.sql
--
-- ============================================================

-- Q1: dbt Lineage (Conceptual - No SQL query)
-- Question: If you run 'dbt run --select int_trips_unioned', what models will be built?
-- Answer: A - stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned
-- Explanation: dbt builds upstream dependencies by default

-- Q2: dbt Tests (Conceptual - No SQL query)
-- Question: Test 'accepted_values: [1,2,3,4,5]' on payment_type. New value 6 appears. What happens?
-- Answer: B - dbt will fail the test, returning a non-zero exit code

-- ============================================================
-- Q3: Count fct_monthly_zone_revenue
-- ============================================================
SELECT 
    'Q3' as question,
    COUNT(*) as answer,
    'Total records in fct_monthly_zone_revenue' as description
FROM fct_monthly_zone_revenue;

-- ============================================================
-- Q4: Best performing Green zone in 2020
-- ============================================================
SELECT 
    'Q4' as question,
    pickup_zone as answer,
    SUM(revenue_monthly_total_amount) as total_revenue
FROM fct_monthly_zone_revenue
WHERE service_type = 'Green' 
  AND pickup_year = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;

-- ============================================================
-- Q5: Green taxi trips in October 2019
-- ============================================================
SELECT 
    'Q5' as question,
    SUM(total_monthly_trips) as answer,
    'Total Green taxi trips in October 2019' as description
FROM fct_monthly_zone_revenue
WHERE service_type = 'Green' 
  AND pickup_year = 2019 
  AND pickup_month = 10;

-- ============================================================
-- Q6: FHV records after filtering
-- ============================================================
SELECT 
    'Q6' as question,
    COUNT(*) as answer,
    'FHV records with dispatching_base_num NOT NULL' as description
FROM stg_fhv_tripdata;

-- ============================================================
-- Summary: All Answers in One Query
-- ============================================================
WITH answers AS (
    SELECT 'Q3' as question, CAST(COUNT(*) AS VARCHAR) as answer
    FROM fct_monthly_zone_revenue
    UNION ALL
    SELECT 'Q4', pickup_zone
    FROM (
        SELECT pickup_zone, SUM(revenue_monthly_total_amount) as rev
        FROM fct_monthly_zone_revenue
        WHERE service_type = 'Green' AND pickup_year = 2020
        GROUP BY 1 ORDER BY 2 DESC LIMIT 1
    )
    UNION ALL
    SELECT 'Q5', CAST(SUM(total_monthly_trips) AS VARCHAR)
    FROM fct_monthly_zone_revenue
    WHERE service_type = 'Green' AND pickup_year = 2019 AND pickup_month = 10
    UNION ALL
    SELECT 'Q6', CAST(COUNT(*) AS VARCHAR)
    FROM stg_fhv_tripdata
)
SELECT * FROM answers;
