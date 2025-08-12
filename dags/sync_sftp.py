from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule

from typing import List
from datetime import timedelta

from helper.discover_files_recursive import discover_files_recursive
from helper.check_sink_files import check_sink_files
from helper.create_sink_directories import create_sink_directories
from helper.sync_files_batch import sync_files_batch
from helper.cleanup_temp_files import cleanup_temp_files

import logging
import pendulum

# DAG Configuration
DAG_ID = "sftp_to_sftp_sync"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 8, 10, tz="UTC"),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 1),
    'catchup': False,
}

# DAG Definition
@dag(
        DAG_ID,
        default_args = default_args,
        description = 'Sync data recursively between two SFTP servers',
        schedule = "0 0 * * *", # 7:00 at VN
        max_active_runs = 1,
        tags = ['sftp', 'sync', 'data-transfer'],
)
def sync_processing():
    # Task 1: Discover files recursively from source
    @task(task_id='discover_files_task')
    def discover_files_task() -> List:
        files_to_sync = discover_files_recursive()
        return files_to_sync

    # Task 2: Check which files need syncing
    @task(task_id='check_sink_files')
    def check_files_task(files_to_sync):
        files_need_sync = check_sink_files(files_to_sync)
        return files_need_sync

    # Task 3: Create necessary directories
    @task(task_id='create_sink_directories')
    def create_dirs_task(files_need_sync):
        create_sink_directories(files_need_sync)

    # Task 4: Sync files
    @task(task_id='sync_files_batch')
    def sync_files_task(files_need_sync):
        sync_files_batch(files_need_sync)

    # Task 5: Cleanup
    @task(
            task_id='cleanup_temp_files',
            trigger_rule=TriggerRule.ALL_DONE
            )
    def cleanup_task():
        cleanup_temp_files()

    files_to_sync = discover_files_task()
    files_need_sync = check_files_task(files_to_sync)
    create_dirs_task(files_need_sync) >> sync_files_task(files_need_sync) >> cleanup_task()


sync_processing()