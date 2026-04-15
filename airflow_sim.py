import time
from datetime import datetime

# 1. THE DAG DEFINITION
# In real Airflow, this would be a DAG object
dag_config = {
    "dag_id": "medical_claims_daily_pipeline",
    "schedule_interval": "@daily",
    "start_date": datetime(2026, 4, 15)
}

# 2. THE TASKS (Operators)
def ingest_data():
    print(f"[{datetime.now()}] TASK 1: Ingesting CSVs from landing zone...")
    time.sleep(2)
    return "SUCCESS"

def transform_data():
    print(f"[{datetime.now()}] TASK 2: Running Spark transformations...")
    time.sleep(3)
    return "SUCCESS"

def load_to_warehouse():
    print(f"[{datetime.now()}] TASK 3: Loading clean data into Star Schema...")
    time.sleep(1)
    return "SUCCESS"

# 3. THE WORKFLOW LOGIC (The "Directed" part of the DAG)
print(f"--- Airflow Scheduler: Triggering {dag_config['dag_id']} ---")

try:
    # Task 1 must run first
    status1 = ingest_data()
    
    # Task 2 only runs IF Task 1 succeeds
    if status1 == "SUCCESS":
        status2 = transform_data()
        
        # Task 3 only runs IF Task 2 succeeds
        if status2 == "SUCCESS":
            status3 = load_to_warehouse()
            print("\n[AIRFLOW] Pipeline Completed Successfully!")
        else:
            raise Exception("Transformation Failed!")
    else:
        raise Exception("Ingestion Failed!")

except Exception as e:
    print(f"\n[AIRFLOW] CRITICAL FAILURE: {e}")
    print("[AIRFLOW] Sending alert email to Data Engineer...")
