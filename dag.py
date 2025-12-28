from airflow import DAG
from airflow.operators.python import PythonOperator  # ty:ignore[unresolved-import]
import pendulum  # ty:ignore[unresolved-import]
from datetime import timedelta
import sys
import os
import logging

# This is crucial for Docker environments
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, DAGS_FOLDER)

try:
    from extract import GitHubExtractor, REPO_OWNER, REPO_NAME, GITHUB_TOKEN
    from transform import run_transform
    from load import run_load
except ImportError as e:
    logging.error(f"Could not import ETL scripts. Check filenames! Error: {e}")

# 3. Define Default Args
default_args = {
    'owner': 'scytale_candidate',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 4. Define the DAG
with DAG(
    'scytale_compliance_etl',
    default_args=default_args,
    description='ETL pipeline for GitHub compliance monitoring',
    schedule_interval='0 0 * * *',  # ty:ignore[unknown-argument]
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags={'scytale'},
) as dag:

    #  Task 1: Extract 
    def execute_extract():
        # Initialize the class using variables imported from extract.py
        extractor = GitHubExtractor(GITHUB_TOKEN, REPO_OWNER, REPO_NAME)
        extractor.run()

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=execute_extract,
        doc_md="Fetches PR metadata, reviews, and status checks."
    )

    #  Task 2: Transform 
    def execute_transform():
        run_transform()

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=execute_transform,
        doc_md="Validates schema and checks compliance rules."
    )

    #  Task 3: Load 
    def execute_load():
        run_load()

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=execute_load,
        doc_md="Loads data into DuckDB and archives Parquet."
    )

    # Making sure the orderis correct
    extract_task >> transform_task >> load_task