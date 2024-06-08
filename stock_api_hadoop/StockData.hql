-- move stockdata to separate directory
-- hdfs dfs -mkdir -p /data/stockdata
-- hdfs dfs -mv /data/stockdata.csv /data/stockdata/
-- Run with: hive -hiveconf input_data=/data/stockdata -hiveconf job_1_output_path=/data/stockdata-hive-out/job1 -hiveconf final_output_path=/data/stockdata-hive-out/final -f StockData.hql

-- PROCESS 1

DROP TABLE IF EXISTS stockdata_raw;
DROP TABLE IF EXISTS stockdata_filtered;
DROP TABLE IF EXISTS stockdata_transformed;
DROP TABLE IF EXISTS process_1_result;
DROP TABLE IF EXISTS job_1_output;
DROP TABLE IF EXISTS process_2_result;
DROP TABLE IF EXISTS final_output;

-- Create an external table to load the data
CREATE EXTERNAL TABLE stockdata_raw (
    symbol STRING,
    name STRING, 
    sector STRING, 
    industry STRING, 
    `exchange` STRING, 
    exchangeShortName STRING, 
    volAvg DOUBLE, 
    mktCap DOUBLE, 
    companyName STRING, 
    cik STRING, 
    isin STRING, 
    cusip STRING, 
    website STRING, 
    ceo STRING, 
    fullTimeEmployees STRING, 
    address STRING, 
    city STRING, 
    zip STRING, 
    image STRING, 
    date_str STRING, 
    open DOUBLE, 
    high DOUBLE, 
    low DOUBLE, 
    close DOUBLE, 
    adjClose DOUBLE, 
    volume DOUBLE, 
    unadjustedVolume DOUBLE, 
    change DOUBLE, 
    changePercent DOUBLE, 
    vwap DOUBLE, 
    label STRING, 
    changeOverTime DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:input_data}';

-- Filter out header and empty sector values
CREATE TABLE stockdata_filtered AS
SELECT *
FROM stockdata_raw
WHERE symbol != 'symbol' AND sector != '';

-- Convert sector to lower case and replace spaces with underscores
-- Calculate avg and period start
CREATE TABLE stockdata_transformed AS
SELECT 
    symbol, 
    LOWER(REPLACE(sector, ' ', '_')) AS sector,
    TO_DATE(DATE_SUB(TO_DATE(date_str), pmod(DATEDIFF(TO_DATE(date_str), '1900-01-01'), 7))) AS periodStart,
    open, 
    high, 
    low, 
    close, 
    volume,
    (high + low) / 2 AS avg
FROM stockdata_filtered;

-- Group by symbol, sector, and periodStart and calculate average values
CREATE TABLE process_1_result AS
SELECT 
    symbol,
    sector,
    periodStart,
    AVG(low) AS avgLow,
    AVG(high) AS avgHigh,
    AVG(close) AS avgClose,
    AVG(open) AS avgOpen,
    AVG(volume) AS avgVolume,
    AVG(avg) AS avgPrice
FROM stockdata_transformed
GROUP BY symbol, sector, periodStart;

-- Store the result (note: in Hive, this usually means creating another table or storing as an external file)
CREATE EXTERNAL TABLE job_1_output (
    symbol STRING,
    sector STRING,
    periodStart DATE,
    avgLow DOUBLE,
    avgHigh DOUBLE,
    avgClose DOUBLE,
    avgOpen DOUBLE,
    avgVolume DOUBLE,
    avgPrice DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:job_1_output_path}';

INSERT INTO TABLE job_1_output
SELECT * FROM process_1_result;

-- PROCESS 2

-- Group by sector and periodStart and calculate average values
CREATE TABLE process_2_result AS
SELECT 
    sector,
    periodStart,
    AVG(avgLow) AS avgLow,
    AVG(avgHigh) AS avgHigh,
    AVG(avgClose) AS avgClose,
    AVG(avgOpen) AS avgOpen,
    AVG(avgVolume) AS avgVolume,
    AVG(avgPrice) AS avgPrice
FROM process_1_result
GROUP BY sector, periodStart;

-- Store the result (note: in Hive, this usually means creating another table or storing as an external file)
CREATE EXTERNAL TABLE final_output (
    sector STRING,
    periodStart DATE,
    avgLow DOUBLE,
    avgHigh DOUBLE,
    avgClose DOUBLE,
    avgOpen DOUBLE,
    avgVolume DOUBLE,
    avgPrice DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:final_output_path}';

INSERT INTO TABLE final_output
SELECT * FROM process_2_result;
