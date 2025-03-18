CREATE EXTERNAL TABLE IF NOT EXISTS tlc_trip.yellow_tripdata (
    VendorID INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    rate_code_id INT,
    store_and_fwd_flag STRING,
    pu_location_id INT,
    do_location_id INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE
)
STORED AS PARQUET
LOCATION 's3://{bucket_name}/tlc_trip/yellow_tripdata/'
TBLPROPERTIES ('parquet.compression'='SNAPPY')