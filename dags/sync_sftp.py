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
    
    # Task: handle sync small + medium files
    @task(task_id='batch_small_sync', pool='default_pool', queue='default')
    def batch_small_sync_task(files_analysis, strategies):
        if 'batch_small_sync' in strategies:
            return sync_handler.sync_small_files_batch(files_analysis)
        else:
            return None

    # Task: handle sync large files use sequential method
    @task(task_id='sequential_large_sync', queue='large_files', pool='large_files_pool', execution_timeout=timedelta(hours=4))
    def sequential_large_sync_task(files_analysis, strategies):
        if 'sequential_large_sync' in strategies:
            return sync_handler.sync_large_files_sequential(files_analysis)
        else:
            return None
        
    # Task: handle sync large files use parallel method
    @task(task_id='parallel_large_sync', queue='large_files', pool='large_files_pool', execution_timeout=timedelta(hours=6))
    def parallel_large_sync_task(files_analysis, strategies):
        if 'parallel_large_sync' in strategies:
            return sync_handler.sync_large_files_parallel(files_analysis)
        else:
            return None
        
    # Task: handle oversized files
    @task(task_id='handle_oversized_files', queue='monitoring')
    def handle_oversized_task(files_analysis, strategies):
        if 'handle_oversized_files' in strategies:
            return sync_handler.handle_oversized_files(files_analysis)
        else:
            return None
        
    ##### END OF SYNC STRATEGY #####
    
    # Convergence point for all sync paths
    @task(task_id='sync_complete', trigger_rule='none_failed')
    def sync_complete():
        pass
    
    # Task: monitor sync progress
    @task(task_id='monitor_sync_progress', trigger_rule='none_failed', queue='monitoring')
    def monitor_task(files_analysis):
        sync_handler.monitor_sync_progress(files_analysis)

    # Task: cleanup temp files
    @task(task_id='cleanup_temp_files', trigger_rule='all_done')
    def cleanup_task():
        sync_handler.cleanup_temp_files()



    ##### WORKFLOWS #####
    files_analysis = analyze_files_task()

    # Branch to different sync strategies
    strategies = determine_strategy_task(files_analysis)

    # All sync tasks run in parallel, each checking if it should execute
    batch_task = batch_small_sync_task(files_analysis, strategies)
    sequential_task = sequential_large_sync_task(files_analysis, strategies)
    parallel_task = parallel_large_sync_task(files_analysis, strategies)
    oversized_task = handle_oversized_task(files_analysis, strategies)

    # All sync tasks converge to sync_complete
    [batch_task, sequential_task, parallel_task, oversized_task] >> sync_complete()

    # Monitoring and cleanup run after sync completion
    sync_complete() >> monitor_task(files_analysis) >> cleanup_task()
    ##### END OF WORKFLOWS #####

sync_processing()