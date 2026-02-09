-- Question 6: Query to retrieve distinct VendorIDs between dates
-- Compare bytes estimate on non-partitioned vs partitioned table

-- Query on NON-PARTITIONED table (check bytes estimate)
SELECT DISTINCT VendorID
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15 23:59:59';
-- Note: This query will scan the entire table since it's not partitioned, resulting in a high bytes estimate.
-- Answer: 310.24 MB

-- Query on PARTITIONED table (check bytes estimate)
SELECT DISTINCT VendorID
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata_partitioned_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15 23:59:59';
-- Note: This query will only scan the relevant partitions for the specified date range, resulting in a much lower bytes estimate.
--  Abswer: 26.84 MB
