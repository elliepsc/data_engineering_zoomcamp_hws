/*
Mart model: Zones Dimension Table
Objective: NYC zones lookup table
Source: Seed file taxi_zone_lookup.csv
*/

{{
  config(
    materialized='table',
    tags=['marts', 'dimensions']
  )
}}

with zone_lookup as (
    -- Reference to seed file (to be loaded)
    select * from {{ ref('taxi_zone_lookup') }}
),

renamed as (
    select
        locationid as location_id,
        borough,
        zone,
        service_zone
    from zone_lookup
)

select * from renamed

/*
Notes:
- Seed file = static CSV in seeds/
- dbt seed loads CSV into BigQuery
- Enables JOIN with fct_trips via location_id
- Required for Q4 (zone names)
*/
