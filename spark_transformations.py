from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sum as _sum

print("--- Running Spark DataFrame Transformations ---")

# 1. Initialize Spark
spark = SparkSession.builder \
    .appName("Medical_Claims_Transformations") \
    .master("local[*]") \
    .getOrCreate()

# 2. Load the 1 Million row dataset
file_path = '~/med_claims_d/landing/large_claims_1M.csv'
import os
file_path = os.path.expanduser(file_path)

print("Loading 1 Million rows into memory...\n")
df = spark.read.csv(file_path, header=True, inferSchema=True)

# ==========================================
# TRANSFORMATIONS (Building the Blueprint)
# ==========================================

# A. FILTER: Keep only "Approved" or "Pending" claims (drop "Denied")
approved_df = df.filter(col("status") != "Denied")

# B. WITHCOLUMN: Add a new column calculating a 5% processing fee
fee_df = approved_df.withColumn("processing_fee", round(col("claim_amount") * 0.05, 2))

# C. GROUPBY: Aggregate the total claim amounts by Diagnosis
summary_df = fee_df.groupBy("diagnosis") \
    .agg(
        _sum("claim_amount").alias("total_payout"),
        _sum("processing_fee").alias("total_fees_collected")
    ) \
    .orderBy("total_payout", ascending=False)

# ==========================================
# ACTIONS (Executing the Blueprint)
# ==========================================

print("=== Final Aggregated Report ===")
# Calling .show() triggers Spark to distribute the filter, math, and grouping across all CPU cores
summary_df.show()

spark.stop()
