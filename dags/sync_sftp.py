from airflow.sdk import dag
from airflow.operators.python import PythonOperator
from helper.discover_files_recursive import discover_files_recursive

# DAG Definition
@dag
def sync_processing():
    # Task 1: Discover files recursively from source
    discover_files_task = PythonOperator(
        task_id='discover_files_recursive',
        python_callable=discover_files_recursive,
)

sync_processing()