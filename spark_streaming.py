from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

spark = SparkSession.builder \
    .appName("Medical_Claims_Streaming") \
    .master("local[*]") \
    .getOrCreate()

# 1. Define the Schema
# In streaming, Spark needs to know the "shape" of the data before it starts
schema = StructType([
    StructField("claim_id", StringType(), True),
    StructField("diagnosis", StringType(), True),
    StructField("claim_amount", DoubleType(), True)
])

# 2. Setup the "Read Stream"
# This tells Spark to watch the folder for any new CSV files
input_path = os.path.expanduser('~/med_claims_d/stream_input')

print(f"Monitoring folder: {input_path} for new claims...")

streaming_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .csv(input_path)

# 3. Apply Transformations
# Let's add a processing timestamp and filter high-value claims
processed_df = streaming_df \
    .withColumn("ingested_at", current_timestamp()) \
    .filter(col("claim_amount") > 500)

# 4. Setup the "Write Stream"
# We will output the results directly to the console (terminal)
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Keep the stream running until you stop it with CTRL+C
query.awaitTermination()
