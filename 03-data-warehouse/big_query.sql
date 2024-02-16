
-- Creating external table referring to gcs path

CREATE OR REPLACE EXTERNAL TABLE `nyc_taxi.green_taxi_2022`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://mage-zoom-omar/green_taxi_2022/green_tripdata_2022-*.parquet']
);

-- Create a non partitioned table from external table

CREATE OR REPLACE TABLE `nyc_taxi.green_taxi_2022_non_partiioned`
AS SELECT * FROM `nyc_taxi.green_taxi_2022`;


SELECT COUNT(*) FROM `nyc_taxi.green_taxi_2022_non_partiioned`;

-- Query to count the distinct number of PULocationIDs
SELECT COUNT( DISTINCT PULocationID) FROM `nyc_taxi.green_taxi_2022`;

SELECT COUNT( DISTINCT PULocationID) FROM `nyc_taxi.green_taxi_2022_non_partiioned`;

-- Query how many records have a fare_amount of 0

SELECT COUNT(*) FROM `nyc_taxi.green_taxi_2022_non_partiioned`
WHERE fare_amount = 0;

-- Create a partitioned table from external table

CREATE TABLE `nyc_taxi.green_taxi_2022_optimized`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT *
FROM `nyc_taxi.green_taxi_2022`;

-- query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022

SELECT DISTINCT PULocationID 
FROM `nyc_taxi.green_taxi_2022_non_partiioned`
WHERE DATE(Lpep_pickup_datetime) BETWEEN '2022-01-06' AND '2022-06-30';

SELECT DISTINCT PULocationID 
FROM `nyc_taxi.green_taxi_2022_optimized`
WHERE DATE(Lpep_pickup_datetime) BETWEEN '2022-01-06' AND '2022-06-30';



