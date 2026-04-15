from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import time

spark = SparkSession.builder \
    .appName("Spark_Optimization") \
    .master("local[*]") \
    .getOrCreate()

file_path = os.path.expanduser('~/med_claims_d/landing/large_claims_1M.csv')
df = spark.read.csv(file_path, header=True, inferSchema=True)

# ---------------------------------------------------------
# 1. THE POWER OF CACHING
# ---------------------------------------------------------
print("--- Test 1: Caching ---")

# First run (Slow - reads from disk)
start = time.time()
count1 = df.count()
print(f"First count (Disk): {time.time() - start:.4f} seconds")

# Cache the data in RAM
df.cache()
df.count() # This "warms up" the cache

# Second run (Fast - reads from RAM)
start = time.time()
count2 = df.count()
print(f"Second count (RAM Cache): {time.time() - start:.4f} seconds")


# ---------------------------------------------------------
# 2. THE POWER OF PARTITIONING
# ---------------------------------------------------------
print("\n--- Test 2: Partitioning ---")

# We will save the data to a 'Parquet' file format (much better than CSV)
# and partition it by 'status'.
output_path = os.path.expanduser('~/med_claims_d/processing/partitioned_claims')

print("Saving partitioned data to disk...")
df.write.mode("overwrite").partitionBy("status").parquet(output_path)

# Now, let's see how Spark reads only the 'Approved' folder instead of the whole dataset
start = time.time()
approved_only = spark.read.parquet(output_path).filter(col("status") == "Approved")
approved_count = approved_only.count()
print(f"Partitioned filter time: {time.time() - start:.4f} seconds")

# 3. Inspect the directory to see the partitions
print("\nLook at how the files are physically stored:")
os.system(f"ls -R {output_path} | grep 'status='")

spark.stop()
