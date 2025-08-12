from airflow.sdk import dag, task, Context
from airflow.operators.python import PythonOperator
from helper.discover_files_recursive import discover_files_recursive
from helper.check_sink_files import check_sink_files
from typing import List

import logging

# DAG Definition
@dag
def sync_processing():
    # Task 1: Discover files recursively from source
    @task(task_id='discover_files_task')
    def discover_files_task() -> List:
        data = discover_files_recursive()
        return data

    # Task 2: Check which files need syncing
    @task(task_id='check_sink_files')
    def check_files_task(data):
        check_sink_files(data)

    data = discover_files_task()
    #files = []
    #files.extend(x['remote_path'] for x in data)
    check_files_task(data)

sync_processing()