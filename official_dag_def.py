from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# 1. Default Arguments
# These settings apply to all tasks in the DAG
default_args = {
    'owner': 'medical_data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Define the DAG
with DAG(
    dag_id='med_claims_processing_v1',
    default_args=default_args,
    description='A formal pipeline for hospital claim ingestion',
    schedule_interval='@daily',
    start_date=datetime(2026, 4, 1),
    catchup=False,
) as dag:

    # 3. Define the Task Functions
    def run_ingestion():
        print("Executing Batch Ingestion Script...")
        # In a real DAG, this would trigger your batch_ingestion.py

    def run_spark_transform():
        print("Executing PySpark Optimization Script...")
        # In a real DAG, this would trigger your spark_transformations.py

    # 4. Create the Operators (The Tasks)
    task_ingest = PythonOperator(
        task_id='ingest_raw_claims',
        python_callable=run_ingestion,
    )

    task_spark = PythonOperator(
        task_id='transform_with_spark',
        python_callable=run_spark_transform,
    )

    # 5. Define the Dependencies
    # This bit of "Bitshift" syntax is what makes Airflow famous.
    # It tells Airflow: Run Ingest FIRST, then Spark.
    task_ingest >> task_spark
