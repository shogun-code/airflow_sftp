import logging, os, time

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.exceptions import AirflowException, AirflowSkipException

from typing import Dict, List, Optional, Tuple, Any
from .common import *


class AdaptiveSFTPSync:
    """Handles SFTP synchronization with adaptive strategies for different file sizes."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
    def analyze_source_files(self) -> Dict:
        """Scan source SFTP and categorize files by size for adaptive processing."""
        source_hook = SFTPHook(ssh_conn_id = SFTP_SOURCE_CONN_ID)
        
        try:
            files_analysis = {
                'small_files': [],  # < 100MB
                'medium_files': [], # 100MB - 1GB
                'large_files': [],  # 1GB - 10GB
                'oversized_files': [], # > 10GB
                'total_size': 0,
                'file_count': 0,
                'anomalies': []
            }
            
            remote_path = SOURCE_BASE_PATH
            
            with source_hook.get_conn() as sftp:
                # Recursive file discovery
                all_files = self._discover_files_recursive(sftp, remote_path)
                
                for file_path, file_stat in all_files:
                    file_size_bytes = file_stat.st_size
                    file_size_mb = file_size_bytes / (1024 * 1024)
                    file_size_gb = file_size_mb / 1024
                    
                    file_info = {
                        'filename': file_stat.filename,
                        'relative_path': os.path.relpath(file_path, SOURCE_BASE_PATH),
                        'remote_path': file_path,
                        'size_bytes': file_size_bytes,
                        'size_mb': file_size_mb,
                        'size_gb': file_size_gb,
                        'modified_time': file_stat.st_mtime,
                        'checksum': None  # Will be calculated if needed
                    }
                    
                    # Categorize files by size
                    if file_size_mb < SMALL_FILE_THRESHOLD_MB:
                        files_analysis['small_files'].append(file_info)
                    elif file_size_gb < LARGE_FILE_THRESHOLD_GB:
                        files_analysis['medium_files'].append(file_info)
                    elif file_size_gb <= MAX_FILE_SIZE_GB:
                        files_analysis['large_files'].append(file_info)
                    else:
                        files_analysis['oversized_files'].append(file_info)
                        self.logger.warning(f"Oversized file detected: {file_path} ({file_size_gb:.2f}GB)")
                    
                    files_analysis['total_size'] += file_size_bytes
                    files_analysis['file_count'] += 1
                    
                    # Detect anomalies
                    self._detect_anomalies(file_info, files_analysis['anomalies'])
            
            self.logger.info(f"File analysis complete: {files_analysis['file_count']} files, "
                           f"{files_analysis['total_size'] / (1024**3):.2f}GB total")
            
            return files_analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing remote files: {str(e)}")
            raise AirflowException(f"File analysis failed: {str(e)}")
    
    def _discover_files_recursive(self, sftp, path: str) -> List[Tuple[str, Any]]:
        """Recursively discover all files in SFTP directory."""
        files = []
        try:
            items = sftp.listdir_attr(path)
            for item in items:
                item_path = os.path.join(path, item.filename).replace('\\', '/')
                #if stat.S_ISDIR(item.st_mode):
                if item.st_mode and (item.st_mode & 0o040000):  # Directory
                    files.extend(self._discover_files_recursive(sftp, item_path))
                else:
                    files.append((item_path, item))
        except Exception as e:
            self.logger.warning(f"Error accessing {path}: {str(e)}")
        return files
    
    def _detect_anomalies(self, file_info: Dict, anomalies: List):
        """Detect potential anomalies in file characteristics."""
        # Size-based anomalies
        if file_info['size_gb'] > LARGE_FILE_THRESHOLD_GB * 10:  # 10x larger than threshold
            anomalies.append({
                'type': 'extreme_size',
                'file': file_info['remote_path'],
                'size_gb': file_info['size_gb'],
                'message': f"File is extremely large: {file_info['size_gb']:.2f}GB"
            })
        
        # Age-based anomalies (files older than 1 year or modified in future)
        current_time = time.time()
        file_age_days = (current_time - file_info['modified_time']) / 86400
        
        if file_age_days > 365:
            anomalies.append({
                'type': 'old_file',
                'file': file_info['remote_path'],
                'age_days': file_age_days,
                'message': f"File is very old: {file_age_days:.0f} days"
            })
        elif file_info['modified_time'] > current_time + 86400:  # Future timestamp
            anomalies.append({
                'type': 'future_timestamp',
                'file': file_info['remote_path'],
                'message': "File has future modification timestamp"
            })
    
    def determine_sync_strategy(self, files_analysis) -> str:
        """Determine the appropriate sync strategy based on file analysis."""
        
        total_large_files = len(files_analysis['large_files'])
        total_oversized_files = len(files_analysis['oversized_files'])
        total_size_gb = files_analysis['total_size'] / (1024**3)
        
        self.logger.info(f"Determining strategy for {files_analysis['file_count']} files, "
                        f"{total_size_gb:.2f}GB total, {total_large_files} large files")
        
        # Handle oversized files separately
        if total_oversized_files > 0:
            return 'handle_oversized_files'
        
        # Strategy selection based on file distribution and total size
        if total_large_files > 10 or total_size_gb > 50:
            return 'parallel_large_sync'
        elif total_large_files > 0:
            return 'sequential_large_sync'
        else:
            return 'batch_small_sync'
    
    def sync_small_files_batch(self, files_analysis):
        """Sync small files in batches for efficiency."""
        source_hook = SFTPHook(ssh_conn_id=SFTP_SOURCE_CONN_ID)
        sink_hook = SFTPHook(ssh_conn_id=SFTP_SINK_CONN_ID)

        small_files = files_analysis['small_files'] + files_analysis['medium_files']

        files_need_sync = self._get_need_sink_files(small_files)
        
        if not files_need_sync:
            raise AirflowSkipException("No small/medium files to sync")
        
        # Create destination directory if needed
        self.create_sink_directories(files_need_sync)
        
        batch_size = 50  # Process 50 files at a time
        
        for i in range(0, len(files_need_sync), batch_size):
            batch = files_need_sync[i:i + batch_size]
            logging.info(f"Processing batch {i//batch_size + 1}: {len(batch)} files")
            
            successful_transfers, failed_transfers = self.sync_files_batch(batch)
        
        logging.info(f"Batch sync complete: {successful_transfers} successful, "
                        f"{failed_transfers} failed")
        
        if failed_transfers > successful_transfers * 0.1:  # More than 10% failure rate
            raise AirflowException(f"High failure rate in batch sync: {failed_transfers}/{successful_transfers + failed_transfers}")
    
    def sync_large_files_sequential(self, files_analysis):
        """Sync large files one by one with progress monitoring."""
        large_files = files_analysis['large_files']
        
        if not large_files:
            raise AirflowSkipException("No large files to sync")
        
        files_need_sync = self._get_need_sink_files(large_files)
        
        # Create destination directory if needed
        self.create_sink_directories(files_need_sync)
        
        for i, file_info in enumerate(files_need_sync):
            self.logger.info(f"Transferring large file {i+1}/{len(files_need_sync)}: "
                           f"{file_info['remote_path']} ({file_info['size_gb']:.2f}GB)")
            
            try:
                # Use chunked transfer for large files
                self._transfer_large_file_chunked(file_info)
            except Exception as e:
                self.logger.error(f"Failed to transfer large file {file_info['remote_path']}: {str(e)}")
                raise AirflowException(f"Large file transfer failed: {str(e)}")
    
    def sync_large_files_parallel(self, files_analysis):
        """Sync large files in parallel using multiple workers."""
        large_files = files_analysis['large_files']
        
        if not large_files:
            raise AirflowSkipException("No large files to sync in parallel")
        
        # This task is routed to the large_files queue with specialized workers
        self.logger.info(f"Starting parallel sync of {len(large_files)} large files")
        
        # Create individual transfer tasks
        for file_info in large_files:
            self._create_parallel_transfer_task(file_info)
    
    def handle_oversized_files(self, files_analysis):
        """Handle oversized files with special processing."""
        oversized_files = files_analysis['oversized_files']
        
        if not oversized_files:
            raise AirflowSkipException("No oversized files to handle")
        
        logging.warning(f"Found {len(oversized_files)} oversized files (>{MAX_FILE_SIZE_GB}GB)")
        
        # Log oversized files and create manual intervention tasks
        for file_info in oversized_files:
            self.logger.warning(f"Oversized file requires manual intervention: "
                              f"{file_info['remote_path']} ({file_info['size_gb']:.2f}GB)")
            
            # Store in XCom for manual processing workflow
            # context['task_instance'].xcom_push(
            #     key=f"oversized_file_{file_info['remote_path'].replace('/', '_')}",
            #     value=file_info
            # )
        
        # Send notification about oversized files
        self._send_oversized_file_notification(oversized_files)
    
    def sync_files_batch(self, files_need_sync):
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

                        # Verify file integrity
                        if not self._verify_file_integrity(local_temp_path, file_info['size_bytes']):
                            raise AirflowException(f"File integrity check failed for {file_info['remote_path']}")
                        
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
                        raise
                        
                    finally:
                        # Clean up temp file
                        if os.path.exists(local_temp_path):
                            os.remove(local_temp_path)
            
            logging.info(f"Sync completed. Success: {synced_count}, Failed: {failed_count}")
            
            if failed_count > 0:
                raise Exception(f"Some files failed to sync. Success: {synced_count}, Failed: {failed_count}")
            
            return synced_count, failed_count
                
        except Exception as e:
            logging.error(f"Error during file sync: {str(e)}")
            raise
    
    def _get_need_sink_files(self, files_to_sync) -> List[Dict[str, Any]]:
        """
        Check which files need to be synced by comparing with sink.
        Returns filtered list of files that need syncing.
        """
        sink_hook = SFTPHook(ssh_conn_id = SFTP_SINK_CONN_ID)
        if not files_to_sync:
            logging.warning("No files discovered from source")
            return []
        
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
                            file_info['size_bytes'] != sink_stat.st_size):
                            needs_sync = True
                            logging.info(f"--File needs update: {sink_path}")
                            logging.info(f"Detail:, source_modified_time: {file_info['modified_time']}, sink_modified_time: {sink_stat.st_mtime}")
                            logging.info(f"Detail:, source_size: {file_info['size_bytes']}, sink_size: {sink_stat.st_size}")
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
    
    def create_sink_directories(self, files_need_sync):
        """
        Create necessary directories in sink SFTP server.
        """
        sink_hook = SFTPHook(ssh_conn_id = SFTP_SINK_CONN_ID)
        if not files_need_sync:
            logging.info("No files to sync, skipping directory creation")
            return
        
        created_dirs = set()
        
        try:
            with sink_hook.get_conn() as sftp_client:
                for file_info in files_need_sync:
                    logging.info(f"[large] file_info = {file_info}")
                    sink_dir = os.path.dirname(file_info['sink_path'])
                    
                    if sink_dir not in created_dirs:
                        try:
                            # Try to create directory (mkdir -p equivalent)
                            self._mkdir_p(sftp_client, sink_dir)
                            created_dirs.add(sink_dir)
                            logging.info(f"Created directory: {sink_dir}")
                        except Exception as e:
                            logging.warning(f"Could not create directory {sink_dir}: {str(e)}")
                            
        except Exception as e:
            logging.error(f"Error creating directories: {str(e)}")
            raise

    def _mkdir_p(self, sftp_client, remote_directory):
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
    
    def _ensure_directory_exists(self, sftp, directory_path: str):
        """Recursively create directory structure on SFTP server."""
        if directory_path == '/' or not directory_path:
            return
            
        try:
            sftp.stat(directory_path)
        except FileNotFoundError:
            # Directory doesn't exist, create parent first
            parent_dir = os.path.dirname(directory_path)
            self._ensure_directory_exists(sftp, parent_dir)
            sftp.mkdir(directory_path)
    
    def _verify_file_integrity(self, file_path: str, expected_size: int) -> bool:
        """Verify file integrity by checking size and calculating checksum if needed."""
        if not os.path.exists(file_path):
            return False
        
        actual_size = os.path.getsize(file_path)
        return actual_size == expected_size
    
    def _transfer_large_file_chunked(self, file_info: Dict):
        """Transfer large files using chunked approach for better reliability."""
        source_hook = SFTPHook(ssh_conn_id=SFTP_SOURCE_CONN_ID)
        sink_hook = SFTPHook(ssh_conn_id=SFTP_SINK_CONN_ID)

        source_path = file_info['remote_path']
        sink_path = os.path.join(SINK_BASE_PATH, file_info['relative_path']).replace('\\', '/')
        chunk_size = CHUNK_SIZE_MB * 1024 * 1024
        
        temp_dir = "/tmp/airflow_large"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = f"{temp_dir}/{os.path.basename(source_path)}"
        
        try:
            total_size = file_info['size_bytes']
            transferred = 0
            
            with source_hook.get_conn() as source_sftp, \
                 sink_hook.get_conn() as sink_sftp:
                
                # Open source file
                with source_sftp.file(source_path, 'rb') as src_file:
                    # Create/open destination file
                    with sink_sftp.file(sink_path, 'wb') as sink_file:
                        while transferred < total_size:
                            remaining = min(chunk_size, total_size - transferred)
                            chunk = src_file.read(remaining)
                            
                            if not chunk:
                                break
                                
                            sink_file.write(chunk)
                            transferred += len(chunk)
                            
                            # Progress logging
                            progress = (transferred / total_size) * 100
                            if transferred % (chunk_size * 10) == 0:  # Log every 10 chunks
                                logging.info(f"Transfer progress for {source_path}: "
                                               f"{progress:.1f}% ({transferred / (1024**3):.2f}GB)")
            
            logging.info(f"Large file transfer completed: {source_path} "
                           f"({file_info['size_gb']:.2f}GB)")
            
        except Exception as e:
            logging.error(f"Chunked transfer failed for {source_path}: {str(e)}")
            # Cleanup incomplete destination file
            try:
                with sink_hook.get_conn() as sink_sftp:
                    sink_sftp.remove(sink_path)
            except:
                pass
            raise

    def _send_oversized_file_notification(self, oversized_files: List[Dict]):
        """Send notification about oversized files requiring manual intervention."""
        message = f"Found {len(oversized_files)} oversized files requiring manual intervention:\n\n"
        for file_info in oversized_files:
            message += f"- {file_info['remote_path']} ({file_info['size_gb']:.2f}GB)\n"
        
        # Log the message (in production, integrate with your notification system)
        logging.warning(message)
        
        # Store notification in XCom for potential downstream processing
        return message
    
    def monitor_sync_progress(self, files_analysis):
        """Monitor overall sync progress and detect issues."""
        
        # Get status from all sync tasks
        sync_status = {
            'total_files': files_analysis['file_count'],
            'total_size_gb': files_analysis['total_size'] / (1024**3),
            'anomalies_detected': len(files_analysis['anomalies']),
            #'start_time': context['ts'],
            'warnings': []
        }
        
        # Check for anomalies and add warnings
        for anomaly in files_analysis['anomalies']:
            sync_status['warnings'].append(anomaly['message'])
        
        # Log monitoring results
        logging.info(f"Sync monitoring - Total: {sync_status['total_files']} files, "
                        f"{sync_status['total_size_gb']:.2f}GB, "
                        f"{sync_status['anomalies_detected']} anomalies detected")
        
        if sync_status['warnings']:
            logging.warning(f"Sync warnings: {sync_status['warnings']}")
        
        return sync_status
    
    def cleanup_temp_files(self):
        """Clean up temporary files and directories."""
        temp_dirs = ['/tmp/airflow_temp', '/tmp/airflow_large']
        
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir):
                import shutil
                try:
                    shutil.rmtree(temp_dir)
                    self.logger.info(f"Cleaned up temporary directory: {temp_dir}")
                except Exception as e:
                    self.logger.warning(f"Failed to clean up {temp_dir}: {str(e)}")