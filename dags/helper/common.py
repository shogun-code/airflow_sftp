from airflow.models import Variable

# Connection IDs (configure these in Airflow UI)
SFTP_SOURCE_CONN_ID = "sftp_source"
SFTP_SINK_CONN_ID = "sftp_sink"

# Configuration variables
SOURCE_BASE_PATH = Variable.get("SOURCE_BASE_PATH", "/upload")
SINK_BASE_PATH = Variable.get("SINK_BASE_PATH", "/dest/data")
TEMP_LOCAL_PATH = Variable.get("TEMP_LOCAL_PATH", "/tmp/sftp_sync")
BATCH_SIZE = int(Variable.get("BATCH_SIZE", "10"))  # Files per batch