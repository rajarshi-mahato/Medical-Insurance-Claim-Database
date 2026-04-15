from pyspark.sql import SparkSession
import os
import time

print("--- Initializing Apache Spark for Medical Insurance Claim Processing D ---")

# 1. Create a Spark Session
# "local[*]" tells Spark to use every available CPU core on your machine to process data in parallel
spark = SparkSession.builder \
    .appName("Medical_Claims_Processing") \
    .master("local[*]") \
    .getOrCreate()

# 2. Define the path to our massive dataset (generated in Task 5)
file_path = os.path.expanduser('~/med_claims_d/landing/large_claims_1M.csv')

print(f"\nLoading data from: {file_path}")
start_time = time.time()

# 3. Read the CSV into a Spark DataFrame
# Spark distributes this data across its "workers" in the background
spark_df = spark.read.csv(file_path, header=True, inferSchema=True)

# 4. Perform Basic Actions
print("\n=== Data Schema ===")
# Spark automatically figures out if a column is text, integer, or decimal
spark_df.printSchema()

print("\n=== Top 5 Rows ===")
# Show prints a cleanly formatted table to the terminal
spark_df.show(5)

print("\n=== Total Row Count ===")
# Count forces Spark to execute the processing across all cores
total_rows = spark_df.count()
end_time = time.time()

print(f"Total Claims Processed: {total_rows:,}")
print(f"Time taken by Spark: {end_time - start_time:.2f} seconds")

# 5. Shut down the cluster to free up memory
spark.stop()
