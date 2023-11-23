from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName('MyFirstPyspark') \
    .getOrCreate()

# Read data from a CSV file
input_file = 'gs://uberproject_data/yellow_tripdata_2023-01.csv'
df = spark.read.csv(input_file, header=True, inferSchema=True)

# Perform some Spark transformations and actions
result = df.groupBy('payment_type')

# Print the column names in your DataFrame
print(df.columns)

# Show the result
print(result)

# Write the result to a Parquet file
df.write.parquet('gs://uberproject_data/uberdata_parq', mode='overwrite')

print('Parquet written succesfully')

# Stop the Spark session
spark.stop()
