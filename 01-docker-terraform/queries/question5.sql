-- Question 5: Find the pickup zone with the largest total_amount on November 18, 2025
SELECT 
    z.zone,
    SUM(t.total_amount) as total_revenue
FROM green_taxi_trips t
JOIN taxi_zones z ON t.pulocationid = z.locationid
WHERE DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z.zone
ORDER BY total_revenue DESC
LIMIT 1;
