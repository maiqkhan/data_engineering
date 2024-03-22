{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *,
        'Green' as service_type
    from {{ ref('stg_staging__green_rides') }}
),
yellow_tripdata as (
    select *,
        'Yellow' as service_type 
    from {{ ref('stg_staging__yellow_rides') }}
    )

trips_union as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
)
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select * 
from trips_union
inner join dim_zones as pickup_zones 
on trips_union.pickup_locationid = pickup_zones.locationid 
inner join dim_zones as dropoff_zones 
on trips_union.dropoff_location_id = dropoff_zones.locationid 