from airflow.models import Variable

# Connection IDs (configure these in Airflow UI)
SFTP_SOURCE_CONN_ID = "sftp_source"
SFTP_SINK_CONN_ID = "sftp_sink"

# Configuration variables
SOURCE_BASE_PATH = Variable.get("SOURCE_BASE_PATH", "/upload")
SINK_BASE_PATH = Variable.get("SINK_BASE_PATH", "/dest/data")
TEMP_LOCAL_PATH = Variable.get("TEMP_LOCAL_PATH", "/tmp/sftp_sync")
TEMP_LOCAL_PATH = "/tmp/sftp_sync"
BATCH_SIZE = int(Variable.get("BATCH_SIZE", "50"))  # Files per batch
CHUNK_SIZE_MB = int(Variable.get('CHUNK_SIZE_MB', 64))
SMALL_FILE_THRESHOLD_MB = int(Variable.get('SMALL_FILE_THRESHOLD_MB', 100)) # 100 MB
LARGE_FILE_THRESHOLD_GB = float(Variable.get('LARGE_FILE_THRESHOLD_GB', 1)) # 1 GB
MAX_FILE_SIZE_GB = float(Variable.get('MAX_FILE_SIZE_GB', 10)) # 10GB
