from airflow.providers.sftp.hooks.sftp import SFTPHook
from typing import List, Dict, Any

import logging, os

# Connection IDs (configure these in Airflow UI)
SFTP_SOURCE_CONN_ID = "sftp_source"
SFTP_SINK_CONN_ID = "sftp_sink"

# Configuration variables
SOURCE_BASE_PATH = "/upload"
SINK_BASE_PATH   = "/upload"
TEMP_LOCAL_PATH  = "/tmp/sftp_sync"

def sync_files_batch(files_need_sync):
    """
    Sync files from source to destination SFTP server.
    """
    
    if not files_need_sync:
        logging.info("No files to sync")
        return
    
    source_hook = SFTPHook(ssh_conn_id = SFTP_SOURCE_CONN_ID)
    sink_hook = SFTPHook(ssh_conn_id = SFTP_SINK_CONN_ID)
    
    # Ensure temp directory exists
    os.makedirs(TEMP_LOCAL_PATH, exist_ok=True)
    
    synced_count = 0
    failed_count = 0
    
    try:
        with source_hook.get_conn() as source_sftp, sink_hook.get_conn() as sink_sftp:
            for file_info in files_need_sync:
                local_temp_path = os.path.join(TEMP_LOCAL_PATH, 
                                             f"temp_{os.path.basename(file_info['filename'])}")
                
                try:
                    # Download from source
                    logging.info(f"Downloading: {file_info['remote_path']} -> {local_temp_path}")
                    source_sftp.get(file_info['remote_path'], local_temp_path)
                    
                    # Upload to destination
                    logging.info(f"Uploading: {local_temp_path} -> {file_info['sink_path']}")
                    sink_sftp.put(local_temp_path, file_info['sink_path'])
                    
                    # Preserve modification time
                    sink_sftp.utime(file_info['sink_path'], 
                                  (file_info['modified_time'], file_info['modified_time']))
                    
                    synced_count += 1
                    logging.info(f"Successfully synced: {file_info['sink_path']}")
                    
                except Exception as e:
                    failed_count += 1
                    logging.error(f"Failed to sync {file_info['remote_path']}: {str(e)}")
                    
                finally:
                    # Clean up temp file
                    if os.path.exists(local_temp_path):
                        os.remove(local_temp_path)
        
        logging.info(f"Sync completed. Success: {synced_count}, Failed: {failed_count}")
        
        if failed_count > 0:
            raise Exception(f"Some files failed to sync. Success: {synced_count}, Failed: {failed_count}")
            
    except Exception as e:
        logging.error(f"Error during file sync: {str(e)}")
        raise