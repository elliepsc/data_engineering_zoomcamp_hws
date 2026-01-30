-- ============================================================
-- Module 4 Homework - 2025 Level (Advanced Analytics)
-- SQL Queries for bonus analytics models
-- ============================================================
-- 
-- Usage:
--   Option 1: Copy-paste into DuckDB CLI
--   Option 2: Run entire file: duckdb taxi_rides_ny.duckdb < homework_2025.sql
--   Option 3: Run in container: docker-compose exec dbt duckdb taxi_rides_ny.duckdb < homework_2025.sql
--
-- Skills demonstrated:
--   - Window functions (LAG, PERCENTILE_CONT)
--   - Financial metrics (MoM growth)
--   - Statistical analysis (percentiles)
--   - dbt macros usage
--
-- ============================================================

-- ============================================================
-- Fare Percentiles Analysis - April 2020
-- Demonstrates: PERCENTILE_CONT, statistical analysis
-- ============================================================
SELECT 
    'Fare Percentiles' as analysis,
    service_type,
    p90_fare as p90,
    p95_fare as p95,
    p97_fare as p97,
    trip_count as trips
FROM fct_monthly_fare_percentiles
WHERE year = 2020 AND month_number = 4
ORDER BY service_type;

--SELECT 
--    service_type,
--    p90_fare,
--    p95_fare,
--    p97_fare,
--    trip_count
--FROM fct_monthly_fare_percentiles
--WHERE year = 2020 AND month_number = 4
--ORDER BY service_type;


-- ============================================================
-- Top 5 Growing Zones - January 2020
-- Demonstrates: LAG window function, MoM calculations
-- ============================================================
SELECT 
    'Top Growth' as analysis,
    pickup_zone,
    service_type,
    revenue as jan_2020_revenue,
    prev_month_revenue as dec_2019_revenue,
    mom_growth_pct as growth_percent
FROM fct_monthly_revenue_growth
WHERE year = 2020 
  AND month_number = 1 
  AND mom_growth_pct IS NOT NULL
ORDER BY mom_growth_pct DESC
LIMIT 5;

--SELECT 
--    pickup_zone,
--    service_type,
--    revenue,
--    prev_month_revenue,
--    mom_growth_pct
--FROM fct_monthly_revenue_growth
--WHERE year = 2020 
--  AND month_number = 1 
--  AND mom_growth_pct IS NOT NULL
--ORDER BY mom_growth_pct DESC
--LIMIT 5;

-- ============================================================
-- Top 5 Declining Zones - January 2020
-- Business insight: Which zones lost revenue
-- ============================================================
SELECT 
    'Top Declines' as analysis,
    pickup_zone,
    service_type,
    mom_growth_pct as decline_percent
FROM fct_monthly_revenue_growth
WHERE year = 2020 
  AND month_number = 1 
  AND mom_growth_pct IS NOT NULL
ORDER BY mom_growth_pct ASC
LIMIT 5;

--SELECT 
--    pickup_zone,
--    service_type,
--    mom_growth_pct
--FROM fct_monthly_revenue_growth
--WHERE year = 2020 
--  AND month_number = 1 
--  AND mom_growth_pct IS NOT NULL
--ORDER BY mom_growth_pct ASC
--LIMIT 5;

-- ============================================================
-- Revenue Volatility Analysis
-- Find zones with highest revenue swings
-- ============================================================
WITH volatility AS (
    SELECT 
        pickup_zone,
        service_type,
        STDDEV(mom_growth_pct) as revenue_volatility,
        AVG(revenue) as avg_monthly_revenue,
        COUNT(*) as months_tracked
    FROM fct_monthly_revenue_growth
    WHERE mom_growth_pct IS NOT NULL
    GROUP BY pickup_zone, service_type
    HAVING COUNT(*) >= 12  -- At least 1 year of data
)
SELECT 
    'Revenue Volatility' as analysis,
    pickup_zone,
    service_type,
    ROUND(revenue_volatility, 2) as volatility_stddev,
    ROUND(avg_monthly_revenue, 0) as avg_revenue,
    months_tracked
FROM volatility
ORDER BY revenue_volatility DESC
LIMIT 10;

--SELECT 
--    pickup_zone,
--    service_type,
--    mom_growth_pct
--FROM fct_monthly_revenue_growth
--WHERE year = 2020 
--  AND month_number = 1 
--  AND mom_growth_pct IS NOT NULL
--ORDER BY mom_growth_pct ASC
--LIMIT 5;


-- ============================================================
-- Payment Method Distribution
-- Demonstrates: dbt macros usage
-- ============================================================
SELECT 
    'Payment Analysis' as analysis,
    payment_method,
    trip_count,
    total_revenue,
    avg_revenue,
    ROUND(100.0 * trip_count / SUM(trip_count) OVER (), 2) as pct_of_trips
FROM fct_payment_analysis
ORDER BY trip_count DESC;

--SELECT 
--    payment_method,
--    trip_count,
--    total_revenue,
 --   avg_revenue
--FROM fct_payment_analysis
--ORDER BY trip_count DESC
--LIMIT 5;

-- ============================================================
-- Year-over-Year Comparison
-- Compare 2019 vs 2020 performance
-- ============================================================
WITH yearly_stats AS (
    SELECT 
        pickup_zone,
        service_type,
        pickup_year,
        SUM(revenue) as annual_revenue,
        SUM(trip_count) as annual_trips
    FROM fct_monthly_revenue_growth
    WHERE pickup_year IN (2019, 2020)
    GROUP BY pickup_zone, service_type, pickup_year
),
yoy_comparison AS (
    SELECT 
        y2020.pickup_zone,
        y2020.service_type,
        y2019.annual_revenue as revenue_2019,
        y2020.annual_revenue as revenue_2020,
        ROUND(100.0 * (y2020.annual_revenue - y2019.annual_revenue) / y2019.annual_revenue, 2) as yoy_growth
    FROM yearly_stats y2020
    LEFT JOIN yearly_stats y2019 
        ON y2020.pickup_zone = y2019.pickup_zone 
        AND y2020.service_type = y2019.service_type
        AND y2019.pickup_year = 2019
    WHERE y2020.pickup_year = 2020
        AND y2019.annual_revenue > 10000  -- Filter insignificant zones
)
SELECT 
    'YoY Growth' as analysis,
    *
FROM yoy_comparison
ORDER BY yoy_growth DESC
LIMIT 10;

-- ============================================================
-- Seasonality Analysis
-- Which months have highest/lowest revenue
-- ============================================================
SELECT 
    'Seasonality' as analysis,
    month_number,
    CASE month_number
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
    END as month_name,
    service_type,
    AVG(revenue) as avg_monthly_revenue,
    COUNT(DISTINCT pickup_zone) as active_zones
FROM fct_monthly_revenue_growth
GROUP BY month_number, service_type
ORDER BY service_type, month_number;

-- ============================================================
-- Summary Statistics
-- ============================================================
SELECT '============================================================' as summary
UNION ALL SELECT 'ADVANCED ANALYTICS COMPLETE'
UNION ALL SELECT 'Skills: Window functions, statistical analysis, financial metrics'
UNION ALL SELECT '============================================================';
