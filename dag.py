from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import asyncio
import sys
import os

# This is crucial for Docker environments
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, DAGS_FOLDER)

from extract import GitHubExtractor, REPO_OWNER, REPO_NAME, GITHUB_TOKEN  # noqa: E402
from transform import run_transform  # noqa: E402
from load import run_load  # noqa: E402

# Define Default Args
default_args = {
    'owner': 'scytale_candidate',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'scytale_compliance_etl',
    default_args=default_args,
    description='ETL pipeline for GitHub compliance monitoring',
    schedule_interval='0 12 * * *', 
    start_date=datetime(2025, 12, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=['scytale'],  
) as dag:

    #  Task 1: Extract 
    def execute_extract(**context):
        extractor = GitHubExtractor(GITHUB_TOKEN, REPO_OWNER, REPO_NAME)
        # We capture the return value (the filename)
        file_path = asyncio.run(extractor.run())
        # Returning it here pushes it to XCom automatically
        return file_path

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=execute_extract,
        provide_context=True,
        doc_md="Fetches PR metadata, reviews, and status checks."
    )

    #  Task 2: Transform 
    def execute_transform(**context):
        # GET the file path from Extract task
        ti = context['ti']
        input_file = ti.xcom_pull(task_ids='extract_task')
        
        if not input_file:
            raise ValueError("No input file received from extract_task")

        # Transform on that specific file
        output_file = run_transform(input_path=input_file, output_dir="data")
        
        # return the new file path for the Load task
        return output_file

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=execute_transform,
        provide_context=True,
        doc_md="Validates schema and checks compliance rules."
    )

    #  Task 3: Load 
    def execute_load(**context):
        # Get the filename passed from Transform
        ti = context['ti']
        transformed_file = ti.xcom_pull(task_ids='transform_task')
        
        # Run the load function , Nothing to return, done.
        run_load(input_file=transformed_file)

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=execute_load,
        provide_context=True,
        doc_md="Loads data into DuckDB and archives Parquet."
    )

    # Making sure the order is correct
    extract_task >> transform_task >> load_task