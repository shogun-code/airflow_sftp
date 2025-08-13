from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule

from typing import List
from datetime import timedelta

from helper.discover_files_recursive import discover_files_recursive
from helper.check_sink_files import check_sink_files
from helper.create_sink_directories import create_sink_directories
from helper.sync_files_batch import sync_files_batch
from helper.cleanup_temp_files import cleanup_temp_files
from helper.sync_handler import AdaptiveSFTPSync

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

# Initialize sync handler
sync_handler = AdaptiveSFTPSync()

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
    # Task: analyze source sftp files
    @task(task_id='analyze_remote_files', pool='default_pool')
    def analyze_files_task():
        files_analysis = sync_handler.analyze_source_files()
        return files_analysis
    
    # Task: determine sync strategy
    #           - sequential large file
    #           - batch small file
    #           - over sized files
    #           - parallel large file
    @task.branch(task_id='determine_sync_strategy')
    def determine_strategy_task(files_analysis):
        return sync_handler.determine_sync_strategy(files_analysis)

    # Task: Sync strategy tasks
    @task(task_id='batch_small_sync', pool='default_pool', queue='default',)
    def batch_small_sync_task(files_analysis):
        return sync_handler.sync_small_files_batch(files_analysis)

    # Task: sequential sync large file
    @task(task_id='sequential_large_sync', queue='large_files', pool='large_files_pool', execution_timeout=timedelta(hours=4))
    def sequential_large_sync_task(files_analysis):
        return sync_handler.sync_large_files_sequential(files_analysis)
        
    # Task:
    @task(task_id='parallel_large_sync', queue='large_files', pool='large_files_pool', execution_timeout=timedelta(hours=6))
    def parallel_large_sync_task(files_analysis):
        return sync_handler.sync_large_files_parallel(files_analysis)
    
    # Task: 
    @task(task_id='handle_oversized_files', queue='monitoring')
    def handle_oversized_task(files_analysis):
        return sync_handler.handle_oversized_files(files_analysis)

    # Task dependencies
    files_analysis = analyze_files_task()

    # Branch to different sync strategies
    determine_strategy_task(files_analysis) >> [
         batch_small_sync_task(files_analysis),
         sequential_large_sync_task(files_analysis),
         parallel_large_sync_task(files_analysis),
         handle_oversized_task(files_analysis)
    ]


sync_processing()