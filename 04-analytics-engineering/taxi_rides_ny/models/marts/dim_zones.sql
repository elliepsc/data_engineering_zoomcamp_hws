

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


