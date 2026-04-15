import pandas as pd
import glob
import os
import transformations as tf  

print("--- Starting Advanced Modular Pipeline ---")

landing_zone = os.path.expanduser('~/med_claims_d/landing/')
csv_files = glob.glob(landing_zone + '*.csv')

dataframes = []
for file in csv_files:
    dataframes.append(pd.read_csv(file))

if not dataframes:
    print("No files found!")
    exit()

raw_df = pd.concat(dataframes, ignore_index=True)
print("\n1. Raw Merged Data Loaded.")

required_cols = ['claim_id', 'patient_name', 'diagnosis', 'claim_amount']

try:
    validated_df = tf.validate_data(raw_df, required_cols)
    
    clean_df = tf.normalize_data(validated_df)
    print("\n2. Data Normalized (Cleaned & Standardized Formats):")
    print(clean_df)
    
    summary_df = tf.aggregate_data(clean_df)
    print("\n3. Data Aggregated (Total Payout by Diagnosis):")
    print(summary_df)
    
except Exception as e:
    print(f"Pipeline Failed: {e}")
