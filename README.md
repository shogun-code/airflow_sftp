# Adaptive SFTP Sync with Airflow & Celery

A production-ready SFTP synchronization solution that automatically scales from kilobytes to terabytes with intelligent anomaly detection and adaptive processing strategies.

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Minimum 4GB RAM, 2 CPU cores
- 20GB free disk space
- SFTP server access credentials

### 1. Initial Setup

```bash
# Clone or create project directory
git clone https://github.com/shogun-code/airflow_sftp.git
cd airflow_sftp

# Create directory structure
mkdir -p logs

# Set Airflow user ID
export AIRFLOW_UID=$(id -u)
echo "AIRFLOW_UID=${AIRFLOW_UID}" > .env
echo "_AIRFLOW_WWW_USER_USERNAME=airflow" >> .env
echo "_AIRFLOW_WWW_USER_PASSWORD=airflow" >> .env
```

### 2. Launch the Stack

```bash
# Start all services
docker compose --profile flower up

# Verify all services are running
docker compose ps
```

Expected output:

```
NAME                                   IMAGE                  COMMAND                  SERVICE                 CREATED        STATUS                  PORTS
airflow_sftp-airflow-apiserver-1       apache/airflow:3.0.0   "/usr/bin/dumb-init …"   airflow-apiserver       21 hours ago   Up 17 hours (healthy)   0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp
airflow_sftp-airflow-dag-processor-1   apache/airflow:3.0.0   "/usr/bin/dumb-init …"   airflow-dag-processor   21 hours ago   Up 17 hours (healthy)   8080/tcp
airflow_sftp-airflow-scheduler-1       apache/airflow:3.0.0   "/usr/bin/dumb-init …"   airflow-scheduler       21 hours ago   Up 17 hours (healthy)   8080/tcp
airflow_sftp-airflow-triggerer-1       apache/airflow:3.0.0   "/usr/bin/dumb-init …"   airflow-triggerer       21 hours ago   Up 17 hours (healthy)   8080/tcp
airflow_sftp-airflow-worker-1          apache/airflow:3.0.0   "/usr/bin/dumb-init …"   airflow-worker          21 hours ago   Up 17 hours (healthy)   8080/tcp
airflow_sftp-airflow-worker-large-1    apache/airflow:3.0.0   "/usr/bin/dumb-init …"   airflow-worker-large    21 hours ago   Up 17 hours (healthy)   8080/tcp
airflow_sftp-flower-1                  apache/airflow:3.0.0   "/usr/bin/dumb-init …"   flower                  21 hours ago   Up 17 hours (healthy)   0.0.0.0:5555->5555/tcp, [::]:5555->5555/tcp, 8080/tcp
airflow_sftp-postgres-1                postgres:13            "docker-entrypoint.s…"   postgres                21 hours ago   Up 17 hours (healthy)   5432/tcp
airflow_sftp-redis-1                   redis:7.2-bookworm     "docker-entrypoint.s…"   redis                   21 hours ago   Up 17 hours (healthy)   6379/tcp
airflow_sftp-sftp-sink-1               atmoz/sftp             "/entrypoint sink:pa…"   sftp-sink               21 hours ago   Up 17 hours             0.0.0.0:2223->22/tcp, [::]:2223->22/tcp
airflow_sftp-sftp-source-1             atmoz/sftp             "/entrypoint source:…"   sftp-source             21 hours ago   Up 17 hours             0.0.0.0:2222->22/tcp, [::]:2222->22/tcp

```

### 3. Access Web Interfaces

- **Airflow UI**: http://localhost:8080 (airflow/airflow)
- **Flower Monitoring**: http://localhost:5555
- **PostgreSQL**: localhost:5432 (user: airflow/pass: airflow/db: airflow)

## ⚙️ Configuration

### 1. SFTP Connections

Navigate to **Airflow UI → Admin → Connections**

#### Source SFTP Connection

```
Connection Id: sftp_source
Connection Type: SFTP
Host: sftp-source
Username: source
Password: password
Port: 22
Extra: {"key_file": "/opt/airflow/.ssh/id_rsa"} # Optional SSH key
```

#### Sink SFTP Connection

```
Connection Id: sftp_destination
Connection Type: SFTP
Host: sftp-sink
Username: sink
Password: password
Port: 22
Extra: {"key_file": "/opt/airflow/.ssh/id_rsa"} # Optional SSH key
```

### 2. Airflow Variables

Navigate to **Admin → Variables** and create:

| Key                       | Value            | Description                               |
| ------------------------- | ---------------- | ----------------------------------------- |
| `SOURCE_BASE_PATH`        | `/upload`        | Source directory path                     |
| `SINK_BASE_PATH`          | `/upload`        | Sink directory path                       |
| `TEMP_LOCAL_PATH`         | `/tmp/sftp_sync` | Temp sink directory path                  |
| `SFTP_SOURCE_CONN_ID`     | `sftp_source`    | Source connection id                      |
| `SFTP_SINK_CONN_ID`       | `sftp_sink`      | Sink connection id                        |
| `SMALL_FILE_THRESHOLD_MB` | `100`            | Small file threshold in MB                |
| `LARGE_FILE_THRESHOLD_GB` | `1`              | Large file threshold in GB                |
| `MAX_FILE_SIZE_GB`        | `10`             | Maximum large files                       |
| `CHUNK_SIZE_MB`           | `64`             | Transfer chunk size in MB for large file  |
| `BATCH_SIZE`              | `50`             | Process 50 small & medium files at a time |

### 3. Worker Pools

Navigate to **Admin → Pools** and verify:

| Pool Name          | Slots | Description        |
| ------------------ | ----- | ------------------ |
| `default_pool`     | 128   | Standard workers   |
| `large_files_pool` | 4     | Large file workers |

## 🎯 Running Strategy

### Execution Flow

1. **Automatic Scheduling**: DAG runs daily
2. **File Analysis**: Scans source SFTP and categorizes files
3. **Strategy Selection**: Chooses optimal processing method
4. **Execution**: Routes tasks to appropriate workers
5. **Monitoring**: Tracks progress and handles anomalies
6. **Cleanup**: Removes temporary files and logs results

### Processing Strategies

### 1. File Size Anomalies

The system automatically detects and categorizes files based on size thresholds:

- **Small Files**: < 100MB → Batch processing on default queue
- **Medium Files**: 100MB - 1GB → Batch processing with compression
- **Large Files**: 1GB - 10GB → Sequential/parallel processing on specialized workers
- **Oversized Files**: > 10GB → Manual intervention workflow

### 2. Adaptive Processing Strategies

#### Small/Medium File Handling (KB to 100MB)

```python
Strategy: Batch Processing
- Queue: 'default'
- Worker Pool: default_pool (high concurrency)
- Batch Size: 50 files per batch
- Features: Compression enabled, fast parallel transfers, integrity checks
```

#### Large File Handling (1GB to 10GB)

```python
Strategy: Chunked Transfer
- Queue: 'large_files'
- Worker Pool: large_files_pool (lower concurrency, more resources)
- Chunk Size: 64MB
- Features: Progress monitoring, resume capability, integrity checks
```

#### Oversized File Handling (>10GB)

```python
Strategy: Manual Intervention
- Queue: 'monitoring'
- Options: Split, compress, external service, streaming
- Requires: Operator approval and strategy selection
```

### 3. Sync Strategies
The system incorporates intelligent synchronization to ensure data consistency between the source and sink locations. Below are the key strategies employed:
#### Sync if source file is newer or different size compared to sink file
- **Comparison Criteria**:  
  The system compares the **last modified timestamp** and **file size** of the source and sink files.  
  - If the source file is **newer** or has a **different size** than the sink file, it is flagged for synchronization.  
  - Redundant transfers are avoided if the source and sink files are identical.

- **Processing Workflow**:  
  1. Retrieve metadata (size, timestamps) from both source and sink.  
  2. Evaluate differences.  
  3. Queue files needing synchronization.  
  4. Execute transfer using the appropriate processing strategy based on file size (as defined above).  

#### Sync if file doesn't exist at sink
- **Handling Missing Files**:  
  If the sink location does not contain a file present in the source:  
  - The file is automatically marked for transfer.  
  - The appropriate strategy is chosen based on the file size.  

## Scaling Architecture

### 1. Celery Worker Specialization

#### Default Workers

- **Purpose**: Handle small to medium files
- **Concurrency**: 4 workers per node
- **Queue**: 'default', 'monitoring'
- **Memory**: Standard allocation

#### Large File Workers

- **Purpose**: Handle files 1GB-10GB
- **Concurrency**: 2 workers per node (fewer but more resources)
- **Queue**: 'large_files'
- **Memory**: High allocation with dedicated temp storage

### 2. Dynamic Resource Allocation

```python
# Worker configuration scales automatically based on file sizes
if file_size_gb > LARGE_FILE_THRESHOLD_GB:
    # Route to specialized large file workers
    task.queue = 'large_files'
    task.pool = 'large_files_pool'
    task.execution_timeout = timedelta(hours=4)
else:
    # Use default workers for efficient batch processing
    task.queue = 'default'
    task.pool = 'default_pool'
    task.execution_timeout = timedelta(hours=1)
```

## Anomaly Detection Mechanisms

### 1. Size-based Anomalies

- **Extreme Size Detection**: Files >10x normal threshold
- **Growth Pattern Analysis**: Sudden size increases from historical data
- **Resource Impact Assessment**: Memory and storage requirements

### 2. Temporal Anomalies

- **Age Detection**: Files older than 1 year
- **Future Timestamps**: Files with future modification dates
- **Processing Time**: Transfers taking unusually long

### 3. Integrity Anomalies

- **Size Verification**: Compare source and destination file sizes
- **Checksum Validation**: Optional MD5/SHA256 verification for critical files
- **Transfer Interruption**: Detect and handle incomplete transfers
