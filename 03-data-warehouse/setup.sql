-- ========================================
-- Module 3 Homework: BigQuery Setup
-- Data Engineering Zoomcamp 2026
-- ========================================

-- 1. CREATE DATASET
-- Location must match GCS bucket location (europe-west9)
CREATE SCHEMA IF NOT EXISTS `de-zoomcamp-module3-486909.trips_data_all`
OPTIONS (
  location = 'europe-west9',
  description = 'NYC Yellow Taxi data - Module 3 Homework'
);

-- 2. CREATE EXTERNAL TABLE
-- References parquet files in GCS bucket
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-module3-486909.trips_data_all.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp-hw3-2026-ellie/yellow_tripdata_2024-*.parquet']
);

-- Verify external table
SELECT
  COUNT(*) as total_rows,
  MIN(tpep_pickup_datetime) as earliest_trip,
  MAX(tpep_dropoff_datetime) as latest_trip
FROM `de-zoomcamp-module3-486909.trips_data_all.external_yellow_tripdata`;

-- 3. CREATE MATERIALIZED TABLE
-- Copy data from external table into BigQuery
-- NOTE: DO NOT partition or cluster this table (per homework instructions)
CREATE OR REPLACE TABLE `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`
AS
SELECT *
FROM `de-zoomcamp-module3-486909.trips_data_all.external_yellow_tripdata`;

-- Verify materialized table
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT VendorID) as unique_vendors,
  COUNT(DISTINCT PULocationID) as unique_pickup_locations
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;

-- 4. CREATE PARTITIONED AND CLUSTERED TABLE
-- Optimized for queries filtering by date and ordering by VendorID
CREATE OR REPLACE TABLE `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;

-- Verify partitioned table
SELECT
  table_name,
  ROUND(size_bytes / POW(10, 9), 2) as size_gb,
  row_count
FROM `de-zoomcamp-module3-486909.trips_data_all.__TABLES__`
WHERE table_name IN (
  'external_yellow_tripdata',
  'yellow_tripdata',
  'yellow_tripdata_partitioned_clustered'
)
ORDER BY table_name;

-- ========================================
-- HOMEWORK QUERIES
-- ========================================

-- Question 1: Total record count
SELECT COUNT(*) as total_records
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;

-- Question 2: Distinct PULocationIDs (check bytes estimate before running!)
-- External table
SELECT COUNT(DISTINCT PULocationID) as distinct_pickup_locations
FROM `de-zoomcamp-module3-486909.trips_data_all.external_yellow_tripdata`;

-- Materialized table
SELECT COUNT(DISTINCT PULocationID) as distinct_pickup_locations
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;

-- Question 3: Columnar storage demonstration (check bytes estimate!)
-- One column
SELECT PULocationID
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;

-- Two columns
SELECT PULocationID, DOLocationID
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;

-- Question 4: Zero fare trips
SELECT COUNT(*) as zero_fare_trips
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`
WHERE fare_amount = 0;

-- Question 6: Partition benefits (check bytes estimate before running!)
-- Non-partitioned table
SELECT DISTINCT VendorID
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15 23:59:59';

-- Partitioned table
SELECT DISTINCT VendorID
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata_partitioned_clustered`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15 23:59:59';

-- Question 9: COUNT(*) metadata scan (check bytes estimate!)
SELECT COUNT(*) as total_rows
FROM `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;

-- ========================================
-- CLEANUP (Run after homework submission)
-- ========================================

-- Drop all tables (optional - run only if you want to clean up)
-- DROP TABLE IF EXISTS `de-zoomcamp-module3-486909.trips_data_all.external_yellow_tripdata`;
-- DROP TABLE IF EXISTS `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata`;
-- DROP TABLE IF EXISTS `de-zoomcamp-module3-486909.trips_data_all.yellow_tripdata_partitioned_clustered`;
-- DROP SCHEMA IF EXISTS `de-zoomcamp-module3-486909.trips_data_all`;
