-- Question 3: Count short trips (trip_distance <= 1 mile) in November 2025

SELECT COUNT(*) as short_trips_count
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance <= 1;
