import os
import subprocess
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from src.gaz_data import GazsData
from src.gaz_data_processor import GazDataProcessor
from src.gaz_data_parquet import CSVtoParquetProcessor
from src.model import run_model_and_forecast  # Ensure the function is imported

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'aa_gaz_pipeline',
    default_args=default_args,
    description='A simple pipeline to run gaz data scripts',
    schedule_interval=timedelta(days=1),
)

# Define tasks
def download_csv_files():
    download_folder = r'/opt/airflow/dags/data/gazs'
    os.makedirs(download_folder, exist_ok=True)
    gazs_data = GazsData(download_folder)
    gazs_data.download_csv_files()

def gaz_data_processor():
    input_folder = r"/opt/airflow/dags/data/gazs"
    output_folder = r"/opt/airflow/dags/data/gazs_output"
    os.makedirs(output_folder, exist_ok=True)
    processor = GazDataProcessor(input_folder, output_folder)
    processor.process_csv_files()
    
def gaz_data_parquet():
    input_folder_parquet = r'/opt/airflow/dags/data/gazs_output'
    output_folder_parquet = r'/opt/airflow/dags/data/gazs_output_parquet'
    os.makedirs(output_folder_parquet, exist_ok=True)
    processor = CSVtoParquetProcessor(input_folder_parquet, output_folder_parquet)
    processor.process_files()
    processor.concatenate_and_save()

def run_model():
    historical_file_path = r'/opt/airflow/dags/data/gazs_output_parquet/main_data.parquet'
    new_day_file_path = r'/opt/airflow/dags/data/gazs_output_parquet/ZAG_PARIS_combined_output.parquet'
    json_result = run_model_and_forecast(historical_file_path, new_day_file_path)
    with open('/shared_data/metrics.json', 'w+') as f:
        json.dump(json_result, f, indent=4)

task1 = PythonOperator(
    task_id='download_csv_files',
    python_callable=download_csv_files,
    dag=dag,
)

task2 = PythonOperator(
    task_id='gaz_data_processor',
    python_callable=gaz_data_processor,
    dag=dag,
)

task3 = PythonOperator(
    task_id='gaz_data_parquet',
    python_callable=gaz_data_parquet,
    dag=dag,
)

task4 = PythonOperator(
    task_id='run_model',
    python_callable=run_model,
    dag=dag,
)

# Set task dependencies
task1 >> task2 >> task3 >> task4