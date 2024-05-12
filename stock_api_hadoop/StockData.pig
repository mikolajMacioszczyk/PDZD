-- Run with: pig -param input_data=/data/stockdata.csv -param job_1_output_path=/data/stockdata-pig-out/job1 -param final_output_path=/data/stockdata-pig-out/final StockData.pig

-- PROCESS 1

-- Load data
data = LOAD '$input_data' USING PigStorage(',') AS (
    symbol:chararray,
    name:chararray, 
    sector:chararray, 
    industry:chararray, 
    exchange:chararray, 
    exchangeShortName:chararray, 
    volAvg:double, 
    mktCap:double, 
    companyName:chararray, 
    cik:chararray, 
    isin:chararray, 
    cusip:chararray, 
    website:chararray, 
    ceo:chararray, 
    fullTimeEmployees:chararray, 
    address:chararray, 
    city:chararray, 
    zip:chararray, 
    image:chararray, 
    date_str:chararray, 
    open:double, 
    high:double, 
    low:double, 
    close:double, 
    adjClose:double, 
    volume:double, 
    unadjustedVolume:double, 
    change:double, 
    changePercent:double, 
    vwap:double, 
    label:chararray, 
    changeOverTime:double);

-- Filter out header and empty sector values
filtered_data = FILTER data BY symbol != 'symbol' AND sector != '';

-- Convert sector to lower case and replace spaces with underscores
-- Calculate avg and period start
transformed_data = FOREACH filtered_data GENERATE 
    symbol, 
    LOWER(REPLACE(sector, ' ', '_')) AS sector,
    SubtractDuration(ToDate(date_str, 'yyyy-MM-dd'), CONCAT('P', (chararray)(DaysBetween(ToDate(date_str, 'yyyy-MM-dd'), ToDate('1900-01-01', 'yyyy-MM-dd')) % 7), 'D')) AS periodStart,
    open, 
    high, 
    low, 
    close, 
    volume,
    (high + low) / 2 AS avg;

-- Group by symbol, sector, and periodStart
grouped_data = GROUP transformed_data BY (symbol, sector, periodStart);

-- Calculate average values
process_1_result = FOREACH grouped_data {
     avgLow = AVG(transformed_data.low);
     avgHigh = AVG(transformed_data.high);
     avgClose = AVG(transformed_data.close);
     avgOpen = AVG(transformed_data.open);
     avgVolume = AVG(transformed_data.volume);
     avgPrice = AVG(transformed_data.avg);
     GENERATE FLATTEN(group) AS (symbol, sector, periodStart), avgLow, avgHigh, avgClose, avgOpen, avgVolume, avgPrice;
}

-- Store the result
STORE process_1_result INTO '$job_1_output_path' USING PigStorage(',');

-- PROCESS 2

-- TODO

-- Store the result
-- STORE process_1_result INTO '$final_output_path' USING PigStorage(',');