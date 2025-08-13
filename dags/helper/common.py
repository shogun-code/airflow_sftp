from airflow.models import Variable

# Connection IDs (configure these in Airflow UI)
SFTP_SOURCE_CONN_ID = "sftp_source"
SFTP_SINK_CONN_ID = "sftp_sink"

# Configuration variables

SOURCE_BASE_PATH = "/upload"
SINK_BASE_PATH = "/upload"

TEMP_LOCAL_PATH = "/tmp/sftp_sync"
BATCH_SIZE = int(Variable.get("BATCH_SIZE", "10"))  # Files per batch
MAX_PARALLEL_BATCHES = int(Variable.get("MAX_PARALLEL_BATCHES", "5"))
CHUNK_SIZE_MB = int(Variable.get('CHUNK_SIZE_MB', 64))
SMALL_FILE_THRESHOLD_MB = int(Variable.get('SMALL_FILE_THRESHOLD_MB', 100))
LARGE_FILE_THRESHOLD_GB = float(Variable.get('LARGE_FILE_THRESHOLD_GB', 0.1))
MAX_FILE_SIZE_GB = float(Variable.get('MAX_FILE_SIZE_GB', 10))



#SOURCE_BASE_PATH = Variable.get("SOURCE_BASE_PATH", "/upload")
#SINK_BASE_PATH = Variable.get("SINK_BASE_PATH", "/dest/data")
#TEMP_LOCAL_PATH = Variable.get("TEMP_LOCAL_PATH", "/tmp/sftp_sync")