import sqlite3
import os
import pandas as pd

print("--- Building OLAP Data Warehouse (Star Schema) ---")

# Connect to a new Data Warehouse database
db_path = os.path.expanduser('~/med_claims_d/processing/claims_data_warehouse.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# ---------------------------------------------------------
# 1. CREATE DIMENSION TABLES (The Context)
# ---------------------------------------------------------
cursor.execute('''
CREATE TABLE IF NOT EXISTS Dim_Patient (
    patient_sk INTEGER PRIMARY KEY,  -- SK means Surrogate Key (used in Data Warehousing)
    full_name TEXT,
    age_group TEXT,
    city TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Dim_Provider (
    provider_sk INTEGER PRIMARY KEY,
    hospital_name TEXT,
    hospital_type TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Dim_Date (
    date_sk INTEGER PRIMARY KEY,
    full_date TEXT,
    year INTEGER,
    month INTEGER,
    quarter TEXT
)
''')

# ---------------------------------------------------------
# 2. CREATE FACT TABLE (The Numbers)
# ---------------------------------------------------------
cursor.execute('''
CREATE TABLE IF NOT EXISTS Fact_Claims (
    claim_id TEXT PRIMARY KEY,
    patient_sk INTEGER,
    provider_sk INTEGER,
    date_sk INTEGER,
    diagnosis_code TEXT,
    total_claim_amount REAL,
    FOREIGN KEY(patient_sk) REFERENCES Dim_Patient(patient_sk),
    FOREIGN KEY(provider_sk) REFERENCES Dim_Provider(provider_sk),
    FOREIGN KEY(date_sk) REFERENCES Dim_Date(date_sk)
)
''')

# ---------------------------------------------------------
# 3. INSERT DUMMY DATA
# ---------------------------------------------------------
cursor.executemany("INSERT OR IGNORE INTO Dim_Patient VALUES (?, ?, ?, ?)", [
    (1, 'John Doe', 'Adult (18-64)', 'New York'),
    (2, 'Jane Smith', 'Adult (18-64)', 'Boston')
])

cursor.executemany("INSERT OR IGNORE INTO Dim_Provider VALUES (?, ?, ?)", [
    (100, 'General Hospital', 'Public'),
    (200, 'Mercy Clinic', 'Private')
])

cursor.executemany("INSERT OR IGNORE INTO Dim_Date VALUES (?, ?, ?, ?, ?)", [
    (20260415, '2026-04-15', 2026, 4, 'Q2'),
    (20260110, '2026-01-10', 2026, 1, 'Q1')
])

cursor.executemany("INSERT OR IGNORE INTO Fact_Claims VALUES (?, ?, ?, ?, ?, ?)", [
    ('CLM-001', 1, 100, 20260415, 'FLU', 150.00),
    ('CLM-002', 1, 200, 20260110, 'XRAY', 500.00),
    ('CLM-003', 2, 100, 20260415, 'SURG', 8500.00)
])
conn.commit()

# ---------------------------------------------------------
# 4. RUN AN ANALYTICAL (OLAP) QUERY
# ---------------------------------------------------------
print("\n--- Running Business Intelligence Report ---")
# The CEO wants to know: What is the total claim payout per hospital type, broken down by Quarter?
olap_query = """
SELECT 
    d_prov.hospital_type,
    d_date.quarter,
    d_date.year,
    COUNT(f.claim_id) as total_claims_filed,
    SUM(f.total_claim_amount) as total_payout_amount
FROM Fact_Claims f
JOIN Dim_Provider d_prov ON f.provider_sk = d_prov.provider_sk
JOIN Dim_Date d_date ON f.date_sk = d_date.date_sk
GROUP BY d_prov.hospital_type, d_date.quarter, d_date.year
ORDER BY total_payout_amount DESC;
"""

df = pd.read_sql_query(olap_query, conn)
print(df.to_string(index=False))

conn.close()
