-- Question 4: How many records have a fare_amount of 0?
-- Answer options: 128,210 / 546,578 / 20,188,016 / 8,333

SELECT COUNT(*) as zero_fare_trips
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`
WHERE fare_amount = 0;
-- Answer: 8333
