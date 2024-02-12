create external table green_taxi_trips.tripdata
(vendorID int,
 lpep_pickup_datetime timestamp,
 lpep_dropoff_datetime timestamp,
 store_and_fwd_flag varchar(100),
 ratecode_id REAL,
 pulocation_id int,
 dolocation_id int,
 passenger_count REAL,
 trip_distance REAL,
 fare_amount REAL,
 extra REAL,
 mta_tax REAL,
 tip_amount REAL,
 tolls_amount REAL,
 ehail_fee varchar(100),
 improvement_surcharge REAL,
 total_amount REAL,
 payment_type REAL,
 trip_type REAL,
 congestion_surcharge REAL,
 lpep_pickup_date DATE
 )
stored as parquet
location 's3://mage-demo-bucket-mkhan/data/green/';