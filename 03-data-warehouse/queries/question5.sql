-- Question 5: What is the best strategy to optimize table if query always filters
-- on tpep_dropoff_datetime and orders by VendorID?
-- Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID

-- Create the optimized table
CREATE OR REPLACE TABLE `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;
