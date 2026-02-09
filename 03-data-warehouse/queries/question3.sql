-- Question 3: Why are the estimated number of Bytes different when querying
-- one column vs two columns?

-- Query 1: Select only PULocationID
SELECT PULocationID
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;
-- Answers: 155.12

-- Query 2: Select both PULocationID and DOLocationID
SELECT PULocationID, DOLocationID
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;
-- Answer: 310.24
