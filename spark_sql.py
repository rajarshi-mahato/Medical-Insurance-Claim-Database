from pyspark.sql import SparkSession
import os

print("--- Running Spark SQL Analysis ---")

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("Medical_Claims_SQL") \
    .master("local[*]") \
    .getOrCreate()

# 2. Load the dataset
file_path = os.path.expanduser('~/med_claims_d/landing/large_claims_1M.csv')
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 3. Register the DataFrame as a SQL Temporary View
# This allows us to use SQL syntax against the variable 'df'
df.createOrReplaceTempView("claims_table")

# 4. Run SQL Queries
# Let's find the top 10 most expensive Approved claims for Surgery or MRI.
query = """
    SELECT 
        claim_id, 
        diagnosis, 
        claim_amount, 
        status 
    FROM claims_table 
    WHERE status = 'Approved' 
      AND (diagnosis = 'Surgery' OR diagnosis = 'MRI')
    ORDER BY claim_amount DESC 
    LIMIT 10
"""

print("\nExecuting Spark SQL Query...")
results_df = spark.sql(query)

# 5. Show results
results_df.show()

# 6. Bonus: Complex Aggregation with SQL
# Find average claim amount per status, but only for claims > $500
query_agg = """
    SELECT 
        status, 
        COUNT(*) as total_count,
        ROUND(AVG(claim_amount), 2) as avg_amount
    FROM claims_table
    WHERE claim_amount > 500
    GROUP BY status
"""

print("Executing Status Summary Query...")
spark.sql(query_agg).show()

spark.stop()
