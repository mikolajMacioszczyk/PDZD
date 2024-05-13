-- Run with: pig -param input_data=/data/carsdata.csv -param job_1_output_path=/data/carsdata-pig-out/job1 -param final_output_path=/data/carsdata-pig-out/final CarsData.pig

-- PROCESS 1

-- Load data
data = LOAD '$input_data' USING PigStorage() AS (
    vin:chararray,
    vehicleCost:double, 
    odometerTypeCode:int, 
    odometerReading:int, 
    countyName:chararray, 
    ZIP5:int, 
    modelYear:int, 
    makeCode:chararray, 
    modelCode:chararray, 
    vehicleTypeDescription:chararray, 
    newUsedCode:chararray, 
    titleIssueDate:chararray, 
    purchaseDate:chararray);

-- Filter out wrong data
filtered_data = FILTER data BY vehicleCost > 0 AND modelYear > 1900;

-- Store the result
STORE filtered_data INTO '$job_1_output_path' USING PigStorage(',');

-- PROCESS 2

transformed_data = FOREACH filtered_data GENERATE 
    ToDate(purchaseDate, 'yyyy-MM-dd') AS purchaseDate,
    modelYear,
    (int)GetYear(ToDate(purchaseDate, 'yyyy-MM-dd')) - modelYear AS vehicleAge,
    SubtractDuration(ToDate(purchaseDate, 'yyyy-MM-dd'), CONCAT('P', (chararray)(DaysBetween(ToDate(purchaseDate, 'yyyy-MM-dd'), ToDate('1900-01-01', 'yyyy-MM-dd')) % 7), 'D')) AS periodStart;

process_2_result = FOREACH transformed_data GENERATE 
    purchaseDate,
    modelYear,
    vehicleAge,
    periodStart,
    (vehicleAge < 8 ? 'New' : (vehicleAge >= 8 AND vehicleAge <= 40 ? 'Old' : 'Classic')) AS WearAndTear;


-- Store the result
STORE process_2_result INTO '$final_output_path' USING PigStorage(',');