import sqlite3
import os
import pandas as pd

print("--- Initializing Medical Claims Database ---")

# 1. Connect to SQLite (This creates the database file if it doesn't exist)
db_path = os.path.expanduser('~/med_claims_d/processing/claims_database.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 2. Design the Schema (Create Tables)
cursor.execute('''
CREATE TABLE IF NOT EXISTS Patients (
    patient_id INTEGER PRIMARY KEY,
    name TEXT,
    age INTEGER
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS Claims (
    claim_id TEXT PRIMARY KEY,
    patient_id INTEGER,
    diagnosis TEXT,
    claim_amount REAL,
    status TEXT,
    FOREIGN KEY(patient_id) REFERENCES Patients(patient_id)
)
''')

# 3. Insert Dummy Data
# We use INSERT OR IGNORE so you can run the script multiple times without errors
patients_data = [
    (101, 'John Doe', 45), 
    (102, 'Jane Smith', 32), 
    (103, 'Bob Brown', 50),
    (104, 'Alice Green', 28)
]
cursor.executemany("INSERT OR IGNORE INTO Patients VALUES (?, ?, ?)", patients_data)

claims_data = [
    ('C001', 101, 'Flu', 150.00, 'Approved'),
    ('C002', 101, 'X-Ray', 250.00, 'Pending'),
    ('C003', 102, 'Surgery', 5500.00, 'Approved'),
    ('C004', 103, 'Checkup', 85.00, 'Denied'),
    ('C005', 104, 'Flu', 125.00, 'Approved')
]
cursor.executemany("INSERT OR IGNORE INTO Claims VALUES (?, ?, ?, ?, ?)", claims_data)
conn.commit()

# --- SQL QUERIES ---

def run_query(title, query):
    print(f"\n{title}")
    print("-" * 40)
    # Pandas has a great function to read SQL queries directly into a pretty table
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))

# Query 1: Basic SELECT
run_query(
    "1. SELECT: View all columns in the Claims table",
    "SELECT * FROM Claims"
)

# Query 2: WHERE clause
run_query(
    "2. WHERE: Find all claims that are larger than $200",
    "SELECT claim_id, diagnosis, claim_amount FROM Claims WHERE claim_amount > 200"
)

# Query 3: ORDER BY clause
run_query(
    "3. ORDER BY: Sort claims from highest amount to lowest amount",
    "SELECT claim_id, diagnosis, claim_amount FROM Claims ORDER BY claim_amount DESC"
)

# Query 4: GROUP BY clause
run_query(
    "4. GROUP BY: Find the total claim amounts per diagnosis",
    "SELECT diagnosis, SUM(claim_amount) as total_payout FROM Claims GROUP BY diagnosis"
)

# Close connection
conn.close()
