-- This file contains all the sentences required to create the tables and
-- ingest all the data that will be used in the tutorial of Superser and
-- time comparisons of Dataria.land


-- shortcut to create the structure directly from a file
-- the problem is that fields are Nullable by default, and some types can be misintrepreted
-- use it as a way to get the 'create table sentence'
-- CREATE TABLE yellow_taxis
-- Engine = Memory
-- AS SELECT * FROM url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-01.parquet' LIMIT 3;


-- create the table that will store all data
CREATE TABLE nyc_yellow_taxis
(
    VendorID Int64,
    tpep_pickup_datetime DateTime64(3),
    tpep_dropoff_datetime DateTime64(3),
    passenger_count Float64,
    trip_distance Float64,
    RatecodeID Float64,
    store_and_fwd_flag String,
    PULocationID Int64,
    DOLocationID Int64,
    payment_type Int64,
    fare_amount Float64,
    extra Float64,
    mta_tax Float64,
    tip_amount Float64,
    tolls_amount Float64,
    improvement_surcharge Float64,
    total_amount Float64,
    congestion_surcharge Float64,
    Airport_fee Nullable(Float64) --some months have nulls in this column, and other months have this column in lowercase 'airport_fee'
)
ENGINE = MergeTree
ORDER BY (tpep_pickup_datetime, tpep_dropoff_datetime, VendorID);


-- 2020
insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-01.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-02.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-03.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-04.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-05.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-06.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-07.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-08.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-09.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-10.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-11.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-12.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;


--2021
insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-02.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-03.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-04.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-05.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-06.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-07.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-08.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-09.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-10.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-11.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Null as Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-12.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;



--2022
insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-02.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-04.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-05.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-06.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-07.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-08.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-09.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-10.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-11.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-12.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;



--2023
insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-04.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-05.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-06.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-07.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-08.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-09.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

insert into nyc_yellow_taxis
select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-10.parquet', Parquet)
settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

--insert into nyc_yellow_taxis
--select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
--from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-11.parquet', Parquet)
--settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;

--insert into nyc_yellow_taxis
--select VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, Airport_fee
--from url('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-12.parquet', Parquet)
--settings input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1;
