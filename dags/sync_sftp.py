from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule

from typing import List
from datetime import timedelta

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
    @task(task_id='determine_sync_strategy')
    def determine_strategy_task(files_analysis):
        return sync_handler.determine_sync_strategy(files_analysis)
    
     # Generic sync task that handles different strategy types
    @task(task_id='execute_sync_strategy')
    def execute_sync_strategy_task(strategy_name, files_analysis):
        if strategy_name == 'batch_small_sync':
            return sync_handler.sync_small_files_batch(files_analysis)
        elif strategy_name == 'sequential_large_sync':
            return sync_handler.sync_large_files_sequential(files_analysis)
        elif strategy_name == 'parallel_large_sync':
            return sync_handler.sync_large_files_parallel(files_analysis)
        elif strategy_name == 'handle_oversized_files':
            return sync_handler.handle_oversized_files(files_analysis)
        else:
            raise ValueError(f"Unknown strategy: {strategy_name}")
    
    @task(task_id='monitor_sync_progress', trigger_rule='none_failed', queue='monitoring')
    def monitor_task(files_analysis):
        sync_handler.monitor_sync_progress(files_analysis)

    # Convergence point for all sync paths
    @task(task_id='sync_complete', trigger_rule='none_failed')
    def sync_complete():
        pass

    @task(task_id='cleanup_temp_files', trigger_rule='all_done')
    def cleanup_task():
        sync_handler.cleanup_temp_files()

    # Task dependencies
    files_analysis = analyze_files_task()

    # Branch to different sync strategies
    strategies = determine_strategy_task(files_analysis)

    # Dynamic task mapping - creates one task per strategy
    sync_tasks = execute_sync_strategy_task.expand(
        strategy_name=strategies,
        files_analysis=[files_analysis] * len(strategies) if isinstance(strategies, list) else [files_analysis]
    )


    # Monitoring and cleanup run after sync completion
    sync_tasks >> monitor_task(files_analysis) >> cleanup_task()

sync_processing()