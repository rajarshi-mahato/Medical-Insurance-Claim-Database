import pandas as pd
import glob
import os

print("--- Starting Medical Claim Processing D ---")

# Define folder paths
landing_zone = os.path.expanduser('~/med_claims_d/landing/')
output_file = os.path.expanduser('~/med_claims_d/processing/master_claims_merged.csv')

# Find all CSV files in the landing zone
csv_files = glob.glob(landing_zone + '*.csv')
print(f"Found {len(csv_files)} files to process: {csv_files}")

# Read and store each dataframe in a list
dataframes = []
for file in csv_files:
    df = pd.read_csv(file)
    dataframes.append(df)

# Merge (Concatenate) all datasets together
if dataframes:
    merged_df = pd.concat(dataframes, ignore_index=True)
    print("\nRaw Merged Data:")
    print(merged_df)
    
    # Clean Missing Values
    merged_df['diagnosis'] = merged_df['diagnosis'].fillna('Unknown')
    merged_df['claim_amount'] = merged_df['claim_amount'].fillna(0.0)
    
    print("\nCleaned Merged Data:")
    print(merged_df)
    
    # Save the cleaned, master dataset
    merged_df.to_csv(output_file, index=False)
    print(f"\nSuccessfully saved cleaned master file to: {output_file}")
else:
    print("No CSV files found to merge.")
