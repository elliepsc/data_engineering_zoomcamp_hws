/*
Mart model: Monthly Zone Revenue
Objective: Monthly aggregation of revenue and trips by zone
Target questions: Q3, Q4, Q5
*/

{{
  config(
    materialized='table',
    tags=['marts', 'aggregates']
  )
}}

with trips as (
    select * from {{ ref('fct_trips') }}
    -- Filter invalid trips
    where is_invalid_trip = false
),

zones as (
    select * from {{ ref('dim_zones') }}
),

monthly_aggregates as (
    select
        -- Dimensions
        t.service_type,
        t.pickup_year,
        t.pickup_month,
        t.pickup_location_id,
        z.zone as pickup_zone,
        z.borough as pickup_borough,
        
        -- Aggregated metrics
        count(*) as total_monthly_trips,
        sum(t.fare_amount) as revenue_monthly_fare,
        sum(t.total_amount) as revenue_monthly_total_amount,
        avg(t.trip_distance) as avg_trip_distance,
        avg(t.trip_duration_minutes) as avg_trip_duration_minutes

    from trips t
    left join zones z
        on t.pickup_location_id = z.location_id
    
    group by 1, 2, 3, 4, 5, 6
)

select * from monthly_aggregates

/*
Notes:
- GROUP BY year, month, zone = monthly granularity
- LEFT JOIN zones for zone names
- Metrics: trip count, revenue, averages
- Q3: count(*) of this table
- Q4: filter service_type='Green', year=2020, max(revenue)
- Q5: filter service_type='Green', year=2019, month=10, sum(trips)
*/
