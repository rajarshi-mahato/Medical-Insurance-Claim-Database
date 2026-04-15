from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
import os

spark = SparkSession.builder \
    .appName("Log_Streaming_Analysis") \
    .master("local[*]") \
    .getOrCreate()

# 1. Setup the monitoring path
log_dir = os.path.expanduser('~/med_claims_d/logs/')

# 2. Read text files as a stream
# Spark treats each line in the log as a single column called 'value'
raw_logs = spark.readStream \
    .format("text") \
    .load(log_dir)

# 3. Transform: Extract the status from our log format
# Recall our log format: "2026-04-15 16:02:04 - Moved claim_001.csv to processing zone."
# We want to grab the action (e.g., 'Moved', 'Successfully', 'Detected')
log_analysis = raw_logs.select(
    split(col("value"), " - ").getItem(1).alias("message")
).select(
    split(col("message"), " ").getItem(0).alias("action")
)

# 4. Aggregate: Count the actions in real-time
action_counts = log_analysis.groupBy("action").count()

# 5. Output to Console
query = action_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

print(f"Now streaming log analysis from {log_dir}...")
query.awaitTermination()
