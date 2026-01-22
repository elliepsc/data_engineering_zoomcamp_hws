-- Question 6: Find the dropoff zone with the largest tip for pickups from East Harlem North in November 2025

SELECT 
    zdo.zone as dropoff_zone,
    MAX(t.tip_amount) as max_tip
FROM green_taxi_trips t
JOIN taxi_zones zpu ON t.pulocationid = zpu.locationid
JOIN taxi_zones zdo ON t.dolocationid = zdo.locationid
WHERE zpu.zone = 'East Harlem North'
  AND t.lpep_pickup_datetime >= '2025-11-01'
  AND t.lpep_pickup_datetime < '2025-12-01'
GROUP BY zdo.zone
ORDER BY max_tip DESC
LIMIT 1;