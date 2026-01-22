-- Question 4: Find the pickup day with the longest trip distance
-- Exclude trips with distance >= 100 miles (data errors)

SELECT DATE(lpep_pickup_datetime) as pickup_day,
       MAX(trip_distance) as max_distance
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2025-11-01' 
  AND lpep_pickup_datetime < '2025-12-01'
  AND trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;
