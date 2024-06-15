# change column name "date" to "dateCol" :)

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StockDataProcessing") \
    .getOrCreate()

# Define paths (these would be passed as parameters in a real scenario)
input_data = '/data/stockdata.csv'
job_1_output_path = '/data/stockdata-spark-out/job1'
final_output_path = '/data/stockdata-spark-out/final'

# Load data
data = spark.read.csv(input_data, header=True, inferSchema=True)

# Create a temporary view
data.createOrReplaceTempView("stock_data")

# Filter out header and empty sector values
filtered_data = spark.sql("""
    SELECT *
    FROM stock_data
    WHERE symbol != 'symbol' AND sector != ''
""")

# Create a temporary view for filtered data
filtered_data.createOrReplaceTempView("filtered_stock_data")

# Convert sector to lower case, replace spaces with underscores, and calculate period start
transformed_data = spark.sql("""
    SELECT
        symbol,
        LOWER(REPLACE(sector, ' ', '_')) AS sector,
        DATE_SUB(
            TO_DATE(dateCol, 'yyyy-MM-dd'),
            DATEDIFF(TO_DATE(dateCol, 'yyyy-MM-dd'), TO_DATE('1900-01-01', 'yyyy-MM-dd')) % 7
        ) AS periodStart,
        CAST(open AS DOUBLE) AS open,
        CAST(high AS DOUBLE) AS high,
        CAST(low AS DOUBLE) AS low,
        CAST(close AS DOUBLE) AS close,
        CAST(volume AS DOUBLE) AS volume,
        (CAST(high AS DOUBLE) + CAST(low AS DOUBLE)) / 2 AS avg
    FROM filtered_stock_data
""")

# Create a temporary view for transformed data
transformed_data.createOrReplaceTempView("transformed_stock_data")

# Group by symbol, sector, and periodStart
grouped_process_1_data = spark.sql("""
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
    FROM transformed_stock_data
    GROUP BY symbol, sector, periodStart
""")

# Store the result
grouped_process_1_data.write.csv(job_1_output_path, header=True, mode='overwrite')

# Create a temporary view for grouped process 1 data
grouped_process_1_data.createOrReplaceTempView("grouped_process_1_data")

# Group by sector and periodStart
grouped_process_2_data = spark.sql("""
    SELECT
        sector,
        periodStart,
        AVG(avgLow) AS avgLow,
        AVG(avgHigh) AS avgHigh,
        AVG(avgClose) AS avgClose,
        AVG(avgOpen) AS avgOpen,
        AVG(avgVolume) AS avgVolume,
        AVG(avgPrice) AS avgPrice
    FROM grouped_process_1_data
    GROUP BY sector, periodStart
""")

# Store the result
grouped_process_2_data.write.csv(final_output_path, header=True, mode='overwrite')

# Stop Spark session
spark.stop()