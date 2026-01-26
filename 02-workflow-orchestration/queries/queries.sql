-- =============================================================================
-- Homework Queries - NYC Taxi Data Analysis
-- =============================================================================

-- Q3: Total rows Yellow 2020 (all months)
SELECT COUNT(*) as total_yellow_2020
FROM yellow_taxi_trips
WHERE year = 2020;

-- Q4: Total rows Green 2020 (all months)
SELECT COUNT(*) as total_green_2020
FROM green_taxi_trips
WHERE year = 2020;

-- Q5: Total rows Yellow March 2021
SELECT COUNT(*) as yellow_march_2021
FROM yellow_taxi_trips
WHERE year = 2021 AND month = 3;

-- Bonus: Verify data by month
SELECT year, month, COUNT(*) as row_count
FROM yellow_taxi_trips
GROUP BY year, month
ORDER BY year, month;

SELECT year, month, COUNT(*) as row_count
FROM green_taxi_trips
GROUP BY year, month
ORDER BY year, month;