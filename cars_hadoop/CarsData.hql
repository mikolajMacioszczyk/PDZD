-- move stockdata to separate directory
-- hdfs dfs -mkdir -p /data/carsdata
-- hdfs dfs -mv /data/carsdata.csv /data/carsdata/
-- Run with: hive -hiveconf input_data=/data/carsdata -hiveconf job_1_output_path=/data/carsdata-hive-out/job1 -hiveconf final_output_path=/data/carsdata-hive-out/final -f CarsData.hql

-- PROCESS 1

DROP TABLE IF EXISTS cars_raw_data;
DROP TABLE IF EXISTS cars_filtered_data;
DROP TABLE IF EXISTS cars_process_2_result;
DROP TABLE IF EXISTS cars_final_result;

-- Load data
CREATE EXTERNAL TABLE cars_raw_data (
    vin STRING,
    vehicleCost DOUBLE, 
    odometerTypeCode INT, 
    odometerReading INT, 
    countyName STRING, 
    ZIP5 INT, 
    modelYear INT, 
    makeCode STRING, 
    modelCode STRING, 
    vehicleTypeDescription STRING, 
    newUsedCode STRING, 
    titleIssueDate STRING, 
    purchaseDate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:input_data}';

-- Filter out wrong data and store intermediate result
CREATE TABLE cars_filtered_data AS
SELECT *
FROM cars_raw_data
WHERE vehicleCost > 0 AND modelYear > 1900;

-- PROCESS 2

-- Transform data
-- Final process result with wear and tear calculation
CREATE TABLE cars_process_2_result AS
WITH cte AS (
    SELECT
        CAST(purchaseDate AS DATE) AS purchaseDate,
        modelYear,
        YEAR(CAST(purchaseDate AS DATE)) - modelYear AS vehicleAge
    FROM cars_filtered_data
)
SELECT 
    purchaseDate,
    modelYear,
    vehicleAge,
    DATE_SUB(purchaseDate, (DATEDIFF(purchaseDate, '1900-01-01') % 7)) AS periodStart,
    CASE 
        WHEN vehicleAge < 8 THEN 'New'
        WHEN vehicleAge >= 8 AND vehicleAge <= 40 THEN 'Old'
        ELSE 'Classic'
    END AS WearAndTear
FROM cte;

-- Store the final result
-- EXPLAIN
CREATE EXTERNAL TABLE cars_final_result (
    purchaseDate DATE,
    modelYear INT,
    vehicleAge INT,
    periodStart DATE,
    WearAndTear STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:final_output_path}';

INSERT INTO TABLE cars_final_result
SELECT * FROM cars_process_2_result;