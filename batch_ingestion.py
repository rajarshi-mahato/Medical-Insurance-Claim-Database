import pandas as pd
import sqlite3
import glob
import os
import shutil

print("--- Starting Daily Batch Ingestion Pipeline ---")

# 1. Define paths
base_dir = os.path.expanduser('~/med_claims_d/')
landing_zone = os.path.join(base_dir, 'landing/')
archive_zone = os.path.join(base_dir, 'archive/')
db_path = os.path.join(base_dir, 'processing/claims_database.db')

# 2. Connect to Database
conn = sqlite3.connect(db_path)

# 3. Find all pending batches
csv_files = glob.glob(landing_zone + '*.csv')

if not csv_files:
    print("No new batch files found in landing zone. Exiting.")
else:
    print(f"Found {len(csv_files)} files to ingest.")

    # 4. Process each file one by one
    for file in csv_files:
        filename = os.path.basename(file)
        print(f"\nIngesting: {filename}...")
        
        try:
            # Read the CSV
            df = pd.read_csv(file)
            
            # Load into the database (append to existing table)
            df.to_sql('Raw_Claims_Ingested', conn, if_exists='append', index=False)
            
            # Move file to archive so it isn't ingested again tomorrow
            shutil.move(file, os.path.join(archive_zone, filename))
            
            print(f"Success: {filename} ingested and archived.")
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")

# 5. Verify the ingestion
print("\n--- Current Database Records ---")
verify_df = pd.read_sql_query("SELECT * FROM Raw_Claims_Ingested", conn)
print(verify_df.to_string(index=False))

conn.close()
