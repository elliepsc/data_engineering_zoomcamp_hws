-- Question 2: Write a query to count the distinct number of PULocationIDs
-- for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read?

-- Query on EXTERNAL table (check bytes estimate in top right)
SELECT COUNT(DISTINCT PULocationID) as distinct_pickup_locations
FROM `de-zoomcamp-module3-486909.trips_data_all.external_yellow_tripdata`;
-- No need to run the above query, just check the bytes estimate in the top right corner of the query editor.
-- It should be around 0 B


-- Query on MATERIALIZED table (check bytes estimate in top right)
SELECT COUNT(DISTINCT PULocationID) as distinct_pickup_locations
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;
-- no need to run the above query, just check the bytes estimate in the top right corner of the query editor.
-- It should be around 155 MB for Materialized
