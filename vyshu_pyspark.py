from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import broadcast

# Initialize a Spark session
spark = SparkSession.builder.appName("CSVExample").getOrCreate()
sc = spark.sparkContext 

# Load the CSV file into a DataFrame
# Replace 'your_file.csv' with the actual path to your CSV file
df = spark.read.csv('gs://uberproject_data/yellow_tripdata_2023-01.csv', header=True, inferSchema=True)

# Display the schema of the DataFrame to identify the data types
df.printSchema()

# Broadcast the value to all worker nodes
broadcasted_value = sc.broadcast(int(sc.getConf().get("spark.executor.instances")))

# Define a function to process a partition
def process_partition(iter):
    # Access the broadcasted value using .value
    instances = broadcasted_value.value

    # Log information about the broadcasted value
    print("Broadcasted Value: {}".format(instances))

    partition_data = list(iter)
    # Log information about the partition data
    print("Partition Data: {}".format(partition_data))

    # Your processing logic here

    # Assuming you have a column named 'passenger_count' that may contain strings
    # and you want to add 1 to each value in that column
    def process_column(value):
        try:
            # Try to convert the value to an integer and add 1
            return int(value) + 1
        except ValueError:
            # If conversion to int fails, leave the value unchanged
            return value

    # Apply the function to the desired column using the 'withColumn' transformation
    # Replace 'passenger_count' with the actual column name you're working with
    df_result = df.withColumn('passenger_count', process_column(col('passenger_count')))

    # Add a timestamp column
    df_result = df_result.withColumn("timestamp", current_timestamp())

    # Show the updated DataFrame
    df_result.show(truncate=False)

    # Return the processed DataFrame for further processing
    return [row for row in df_result.collect()]

# Apply the process_partition function to each partition using 'mapPartitions'
result_rdd = df.rdd.mapPartitions(process_partition)

# Trigger an action to execute the job
for row in result_rdd.collect():
    pass  # Process the collected rows as needed
