import logging, os

from airflow.providers.sftp.hooks.sftp import SFTPHook
from typing import List, Dict, Any

# Connection IDs (configure these in Airflow UI)
SFTP_SOURCE_CONN_ID = "sftp_source"
SFTP_SINK_CONN_ID = "sftp_sink"

# Configuration variables
SOURCE_BASE_PATH = "/upload"

def discover_files_recursive(**context) -> List[Dict[str, Any]]:
    """
    Recursively discover all files from source SFTP server.
    Returns a list of file metadata dictionaries.
    """
    source_hook = SFTPHook(ssh_conn_id = SFTP_SOURCE_CONN_ID)
    
    def _walk_directory(sftp_client, remote_path: str, files_list: List[Dict[str, Any]]):
        """Recursively walk through directories and collect file information."""
        try:
            for item in sftp_client.listdir_attr(remote_path):
                item_path = os.path.join(remote_path, item.filename).replace('\\', '/')
                
                # Check if it's a directory
                if item.st_mode and (item.st_mode & 0o040000):  # Directory
                    logging.info(f"Found directory: {item_path}")
                    _walk_directory(sftp_client, item_path, files_list)
                else:  # File
                    file_info = {
                        'remote_path': item_path,
                        'filename': item.filename,
                        'size': item.st_size,
                        'modified_time': item.st_mtime,
                        'relative_path': os.path.relpath(item_path, SOURCE_BASE_PATH),
                    }
                    files_list.append(file_info)
                    logging.info(f"Found file: {item_path} (Size: {item.st_size} bytes)")
        except Exception as e:
            logging.error(f"Error walking directory {remote_path}: {str(e)}")
            raise
    
    files_to_sync = []
    
    try:
        with source_hook.get_conn() as sftp_client:
            logging.info(f"Starting file discovery from: {SOURCE_BASE_PATH}")
            _walk_directory(sftp_client, SOURCE_BASE_PATH, files_to_sync)
            
        logging.info(f"Discovered {len(files_to_sync)} files for synchronization")
        
        # Store the file list in XCom for next task
        context['task_instance'].xcom_push(key='files_to_sync', value=files_to_sync)
        
        return files_to_sync
        
    except Exception as e:
        logging.error(f"Error during file discovery: {str(e)}")
        raise