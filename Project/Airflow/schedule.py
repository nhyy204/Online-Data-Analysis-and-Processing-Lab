from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dotenv import load_dotenv, find_dotenv
from datetime import datetime
import os, subprocess

# Load environment variables from a .env file
load_dotenv(find_dotenv())

def push_data():
    serving_path = os.getenv('SERVING_SCRIPT')
    
    result = subprocess.run(
        ["python3", serving_path],
        capture_output=True,
        text=True
    )
    
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)

    if result.returncode != 0:
        raise Exception(f"Script failed with code {result.returncode}: {result.stderr}")


with DAG(
    dag_id = "push_transactions_data_to_powerBI",
    start_date = datetime(2025, 6, 10),
    schedule = "@daily",
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id='run_serving_job',
        python_callable=push_data
    )
