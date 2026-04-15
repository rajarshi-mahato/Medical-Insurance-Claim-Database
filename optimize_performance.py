import pandas as pd
import numpy as np
import time
import os

file_path = os.path.expanduser('~/med_claims_d/landing/large_claims_1M.csv')

print("--- 1. Baseline: Standard Pandas Read ---")
start_time = time.time()
df_standard = pd.read_csv(file_path)
end_time = time.time()

# Calculate deep memory usage in Megabytes (MB)
standard_memory = df_standard.memory_usage(deep=True).sum() / (1024 * 1024)
print(f"Time to read: {end_time - start_time:.2f} seconds")
print(f"Memory usage: {standard_memory:.2f} MB")


print("\n--- 2. Optimized: Pandas with Memory Management ---")
# We explicitly tell Pandas to use smaller data types (e.g., int32 instead of int64)
# and convert repeating text into 'category' types to save massive amounts of RAM.
optimized_dtypes = {
    'claim_id': 'int32',
    'patient_id': 'int32',
    'diagnosis': 'category',
    'claim_amount': 'float32',
    'status': 'category'
}

start_time = time.time()
df_optimized = pd.read_csv(file_path, dtype=optimized_dtypes)
end_time = time.time()

optimized_memory = df_optimized.memory_usage(deep=True).sum() / (1024 * 1024)
print(f"Time to read: {end_time - start_time:.2f} seconds")
print(f"Memory usage: {optimized_memory:.2f} MB")
print(f"-> RAM Saved: {standard_memory - optimized_memory:.2f} MB!")


print("\n--- 3. Performance: Pandas Apply vs NumPy Vectorization ---")
# Task: Calculate a 10% processing fee for all 1 Million claims

# METHOD A: The Standard Way (Pandas .apply) - Iterates row by row
start_time = time.time()
df_optimized['fee_pandas'] = df_optimized['claim_amount'].apply(lambda x: x * 0.10)
end_time = time.time()
pandas_time = end_time - start_time
print(f"Pandas .apply() calculation time: {pandas_time:.4f} seconds")

# METHOD B: The Advanced Way (NumPy Vectorization) - Processes entire arrays at once
start_time = time.time()
df_optimized['fee_numpy'] = df_optimized['claim_amount'].values * 0.10
end_time = time.time()
numpy_time = end_time - start_time
print(f"NumPy Vectorization calculation time: {numpy_time:.4f} seconds")
print(f"-> NumPy was {pandas_time / numpy_time:.2f}x faster!")
