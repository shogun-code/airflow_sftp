import logging, os

from airflow.providers.sftp.hooks.sftp import SFTPHook
from typing import List, Dict, Any

# Connection IDs (configure these in Airflow UI)
SFTP_SINK_CONN_ID = "sftp_sink"

# Configuration variables
SOURCE_BASE_PATH = "/upload"
SINK_BASE_PATH   = "/upload"

def create_sink_directories(files_need_sync):
    """
    Create necessary directories in sink SFTP server.
    """
    
    if not files_need_sync:
        logging.info("No files to sync, skipping directory creation")
        return
    
    sink_hook = SFTPHook(ssh_conn_id = SFTP_SINK_CONN_ID)
    created_dirs = set()
    
    try:
        with sink_hook.get_conn() as sftp_client:
            for file_info in files_need_sync:
                sink_dir = os.path.dirname(file_info['sink_path'])
                
                if sink_dir not in created_dirs:
                    try:
                        # Try to create directory (mkdir -p equivalent)
                        _mkdir_p(sftp_client, sink_dir)
                        created_dirs.add(sink_dir)
                        logging.info(f"Created directory: {sink_dir}")
                    except Exception as e:
                        logging.warning(f"Could not create directory {sink_dir}: {str(e)}")
                        
    except Exception as e:
        logging.error(f"Error creating directories: {str(e)}")
        raise

def _mkdir_p(sftp_client, remote_directory):
    """Create directory recursively (like mkdir -p)."""
    dirs = []
    dir_path = remote_directory
    
    while dir_path != '/':
        dirs.append(dir_path)
        dir_path = os.path.dirname(dir_path)
    
    dirs.reverse()
    
    for directory in dirs:
        try:
            sftp_client.stat(directory)
        except FileNotFoundError:
            try:
                sftp_client.mkdir(directory)
            except Exception:
                pass  # Directory might have been created by another process
