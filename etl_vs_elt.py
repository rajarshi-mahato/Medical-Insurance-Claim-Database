import pandas as pd
import numpy as np
import sqlite3
import time
import os

print("--- ETL vs ELT Pipeline Comparison ---")

# 1. Setup Environment
db_path = os.path.expanduser('~/med_claims_d/processing/etl_elt_test.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Generate 100,000 rows of "dirty" data (missing claim amounts)
print("Generating 100,000 raw medical claims...")
n = 100000
raw_df = pd.DataFrame({
    'claim_id': np.arange(1, n + 1),
    'diagnosis': np.random.choice(['Flu', 'Surgery', 'Checkup'], size=n),
    # 20% of the data will have missing claim amounts (NaN)
    'claim_amount': np.where(np.random.rand(n) > 0.2, np.random.uniform(100, 5000, size=n), np.nan)
})

# ==========================================
# METHOD A: The ETL Pipeline (Python does the Transform)
# ==========================================
print("\nStarting ETL Pipeline...")
start_time_etl = time.time()

# EXTRACT (Done by holding raw_df in memory)
etl_df = raw_df.copy()

# TRANSFORM (Clean missing values using Python/Pandas)
etl_df['claim_amount'] = etl_df['claim_amount'].fillna(0.0)
etl_df['diagnosis'] = etl_df['diagnosis'].str.upper()

# LOAD (Push the clean data into the database)
etl_df.to_sql('etl_clean_claims', conn, if_exists='replace', index=False)

etl_duration = time.time() - start_time_etl
print(f"ETL Pipeline completed in: {etl_duration:.4f} seconds")


# ==========================================
# METHOD B: The ELT Pipeline (Database does the Transform)
# ==========================================
print("\nStarting ELT Pipeline...")
start_time_elt = time.time()

# EXTRACT & LOAD (Push the RAW, dirty data directly into the database)
raw_df.to_sql('elt_raw_claims', conn, if_exists='replace', index=False)

# TRANSFORM (Clean the data using purely SQL inside the database)
transform_query = """
    CREATE TABLE IF NOT EXISTS elt_clean_claims AS
    SELECT 
        claim_id,
        UPPER(diagnosis) AS diagnosis,
        COALESCE(claim_amount, 0.0) AS claim_amount
    FROM elt_raw_claims;
"""
cursor.execute("DROP TABLE IF EXISTS elt_clean_claims") # Clean slate for the test
cursor.execute(transform_query)

elt_duration = time.time() - start_time_elt
print(f"ELT Pipeline completed in: {elt_duration:.4f} seconds")

# ==========================================
# Conclusion
# ==========================================
print("\n--- Results ---")
if etl_duration < elt_duration:
    print(f"ETL was faster by {elt_duration - etl_duration:.4f} seconds (Python Pandas speed).")
else:
    print(f"ELT was faster by {etl_duration - elt_duration:.4f} seconds (Database SQL speed).")

conn.close()
