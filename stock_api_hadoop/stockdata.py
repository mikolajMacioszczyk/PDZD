from pyspark.sql import SparkSession
import pyspark.sql.functions as F

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

# Filter out header and empty sector values
filtered_data = data.filter((data['symbol'] != 'symbol') & (data['sector'] != ''))

# Convert sector to lower case and replace spaces with underscores
# Calculate avg and period start
transformed_data = filtered_data.select(
    F.col('symbol'),
    F.lower(F.regexp_replace(F.col('sector'), ' ', '_')).alias('sector'),
    F.date_sub(
        F.to_date(F.col('date_str'), 'yyyy-MM-dd'),
        F.expr('datediff(to_date(date_str, "yyyy-MM-dd"), to_date("1900-01-01", "yyyy-MM-dd")) % 7')
    ).alias('periodStart'),
    F.col('open').cast('double'),
    F.col('high').cast('double'),
    F.col('low').cast('double'),
    F.col('close').cast('double'),
    F.col('volume').cast('double'),
    ((F.col('high').cast('double') + F.col('low').cast('double')) / 2).alias('avg')
)

# Group by symbol, sector, and periodStart
grouped_process_1_data = transformed_data.groupBy('symbol', 'sector', 'periodStart').agg(
    F.avg('low').alias('avgLow'),
    F.avg('high').alias('avgHigh'),
    F.avg('close').alias('avgClose'),
    F.avg('open').alias('avgOpen'),
    F.avg('volume').alias('avgVolume'),
    F.avg('avg').alias('avgPrice')
)

# Store the result
grouped_process_1_data.write.csv(job_1_output_path, header=True, mode='overwrite')

# Group by sector and periodStart
grouped_process_2_data = grouped_process_1_data.groupBy('sector', 'periodStart').agg(
    F.avg('avgLow').alias('avgLow'),
    F.avg('avgHigh').alias('avgHigh'),
    F.avg('avgClose').alias('avgClose'),
    F.avg('avgOpen').alias('avgOpen'),
    F.avg('avgVolume').alias('avgVolume'),
    F.avg('avgPrice').alias('avgPrice')
)

# Store the result
grouped_process_2_data.write.csv(final_output_path, header=True, mode='overwrite')

# Stop Spark session
spark.stop()
