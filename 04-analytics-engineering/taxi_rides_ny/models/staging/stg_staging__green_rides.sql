{{
    config(
        materialized='view'
    )
}}

with 

tripdata as (

    select *,
        row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
     from {{ source('staging', 'green_rides') }}
     where vendorid is not null

)

select
    --identifiers
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecode_id as integer) as ratecode_id,
    cast(pulocation_id as integer) as pickup_locationid,
    cast(dolocation_id as integer) as dropoff_location_id,
    
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(passenger_count as integer) as passenger_count,

    
    --payment_info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    coalesce(cast(payment_type as integer), 0) as payment_type
    {{get_payment_type_description('payment_type')}} as payment_type_description

from tripdata
where rn = 1



{% if var('is_test_run', default=true)%}

    limit 100 

{% endif %}
