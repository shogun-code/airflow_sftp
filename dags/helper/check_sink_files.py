import logging, os

from airflow.providers.sftp.hooks.sftp import SFTPHook
from typing import List, Dict, Any

# Connection IDs (configure these in Airflow UI)
SFTP_SOURCE_CONN_ID = "sftp_source"
SFTP_SINK_CONN_ID = "sftp_sink"

# Configuration variables
SOURCE_BASE_PATH = "/upload"
SINK_BASE_PATH   = "/upload"

def check_sink_files(files_to_sync) -> List[Dict[str, Any]]:
    """
    Check which files need to be synced by comparing with sink.
    Returns filtered list of files that need syncing.
    """
    if not files_to_sync:
        logging.warning("No files discovered from source")
        return []
    
    sink_hook = SFTPHook(ssh_conn_id = SFTP_SINK_CONN_ID)
    files_need_sync = []
    
    try:
        with sink_hook.get_conn() as sftp_client:
            for file_info in files_to_sync:
                sink_path = os.path.join(SINK_BASE_PATH, file_info['relative_path']).replace('\\', '/')
                needs_sync = False
                
                try:
                    # Check if file exists and compare modification time
                    sink_stat = sftp_client.stat(sink_path)
                    
                    # Sync if source file is newer or different size
                    if (file_info['modified_time'] > sink_stat.st_mtime or 
                        file_info['size'] != sink_stat.st_size):
                        needs_sync = True
                        logging.info(f"--File needs update: {sink_path}")
                        logging.info(f"Detail:, source_modified_time: {file_info['modified_time']}, sink_modified_time: {sink_stat.st_mtime}")
                        logging.info(f"Detail:, source_size: {file_info['size']}, sink_size: {sink_stat.st_size}")
                    else:
                        logging.info(f"File up to date: {sink_path}")
                        
                except FileNotFoundError:
                    # File doesn't exist at sink
                    needs_sync = True
                    logging.info(f"New file to sync: {sink_path}")
                
                if needs_sync:
                    file_info['sink_path'] = sink_path
                    files_need_sync.append(file_info)
    
    except Exception as e:
        logging.error(f"Error checking sink files: {str(e)}")
        raise
    
    logging.info(f"Files needing sync: {len(files_need_sync)}")
    
    return files_need_sync