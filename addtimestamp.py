from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp 
from pyspark.sql.types import IntegerType, StringType, TimestampType
from pyspark import SparkContext

spark = SparkSession.builder.appName('Addtimestamp').getOrCreate()

input_file = 'gs://uberproject_data/yellow_tripdata_2023-01.csv'
rdd = spark.read.csv(input_file, header = True, inferSchema = True)

# Display the schema of the DataFrame to identify the data types
rdd.printSchema()
def addtimestamp():
    # Add a timestamp column with the current timestamp
    result = df.withColumn("timestamp_column", current_timestamp())

result_rdd = rdd.repartition(addtimestamp)
# Collect the results to the driver
result_list = result_rdd.collect()

print(result_list)

# Write the result to GCS
output_file = 'gs://uberproject_data/yellow_data_addtimestamp.csv'
result.write.csv(output_file, mode='overwrite')

# Stop the Spark session
spark.stop()