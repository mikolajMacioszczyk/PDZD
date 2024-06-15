# change column name "date" to "dateCol" :)

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CarsDataProcessing") \
    .getOrCreate()

input_data = '/data/carsdata.csv'
intermediate_output_path = '/data/carsdata-spark-out/intermediate'
final_output_path = '/data/carsdata-spark-out/final'

data = spark.read.csv(input_data, header=True, inferSchema=True, sep='\t')

data.createOrReplaceTempView("cars_raw_data")

filtered_data = spark.sql("""
    SELECT *
    FROM cars_raw_data
    WHERE vehicleCost > 0 AND modelYear > 1900
""")

filtered_data.write.csv(intermediate_output_path, header=True, mode='overwrite')

filtered_data.createOrReplaceTempView("cars_filtered_data")

transformed_data = spark.sql("""
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
    FROM cte
""")

transformed_data.write.csv(final_output_path, header=True, mode='overwrite')

spark.stop()
