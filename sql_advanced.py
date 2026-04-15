import sqlite3
import os
import pandas as pd

print("--- Running Advanced SQL Business Reports ---")

# Connect to the existing database
db_path = os.path.expanduser('~/med_claims_d/processing/claims_database.db')
conn = sqlite3.connect(db_path)

def run_query(title, query):
    print(f"\n{title}")
    print("-" * 50)
    df = pd.read_sql_query(query, conn)
    print(df.to_string(index=False))

# ---------------------------------------------------------
# 1. INNER JOIN
# Objective: The Claims table only has 'patient_id'. We need to JOIN 
# the Patients table to see the actual names of the people.
# ---------------------------------------------------------
query_join = """
SELECT 
    Patients.name, 
    Patients.age, 
    Claims.claim_id, 
    Claims.diagnosis, 
    Claims.claim_amount
FROM Claims
INNER JOIN Patients 
    ON Claims.patient_id = Patients.patient_id;
"""
run_query("1. INNER JOIN: Combine Patients and Claims Tables", query_join)

# ---------------------------------------------------------
# 2. SUBQUERY
# Objective: Find all claims that are higher than the overall 
# average claim amount across the entire hospital.
# ---------------------------------------------------------
query_subquery = """
SELECT 
    claim_id, 
    diagnosis, 
    claim_amount 
FROM Claims 
WHERE claim_amount > (
    SELECT AVG(claim_amount) FROM Claims
);
"""
run_query("2. SUBQUERY: Claims strictly greater than the overall average", query_subquery)

# ---------------------------------------------------------
# 3. WINDOW FUNCTION
# Objective: Show each individual claim, but also calculate the 
# TOTAL cumulative amount that specific patient has claimed.
# Notice how it doesn't collapse the rows like GROUP BY does.
# ---------------------------------------------------------
query_window = """
SELECT 
    p.name, 
    c.claim_id, 
    c.claim_amount,
    SUM(c.claim_amount) OVER(PARTITION BY p.patient_id) as total_patient_lifetime_claims
FROM Claims c
INNER JOIN Patients p 
    ON c.patient_id = p.patient_id;
"""
run_query("3. WINDOW FUNCTION: Individual claims alongside total patient claims", query_window)

# Close connection
conn.close()
