# DataFlow Docker Testing Environment

Local testing stack with all source/target systems for pipeline development.

## Quick Start

```bash
# Start everything
docker compose up -d

# Start only specific services (e.g. just databases + Airflow)
docker compose up -d db redis postgres-source sqlserver mysql airflow-db airflow-init airflow-webserver airflow-scheduler

# Start minimal (app + its dependencies only)
docker compose up -d db redis app
```

## Services & Ports

| Service | Port | Credentials | Purpose |
|---------|------|-------------|---------|
| **DataFlow App** | 5000 | admin / admin | Main application |
| **PostgreSQL (app)** | 5432 | dataflow / dataflow | App database |
| **Redis** | 6379 | (none) | Session/cache store |
| **PostgreSQL (source)** | 5433 | source_user / source_pass | Test source database (schemas: hr, sales) |
| **SQL Server** | 1433 | sa / DataFlow#2026! | Test source database (schemas: hr, finance) |
| **MySQL** | 3306 | source_user / source_pass (root: rootpass) | Test source database (customers, invoices) |
| **MongoDB** | 27017 | root / rootpass | Test source database (events, sensor_readings) |
| **Spark Master** | 8081 (UI), 7077 (RPC) | (none) | Spark cluster master |
| **Spark Worker** | — | (none) | Spark cluster worker (2G RAM, 2 cores) |
| **Airflow** | 8080 | admin / admin | Airflow webserver (dag-factory pre-installed) |
| **SFTP** | 2222 | testuser / testpass | Flat file source testing |
| **MinIO (S3)** | 9000 (API), 9001 (console) | minioadmin / minioadmin | S3-compatible object storage |
| **Azurite** | 10000 (blob), 10001 (queue), 10002 (table) | (default dev creds) | Azure Blob Storage emulator |

## Sample Data

### PostgreSQL Source (port 5433)
- `hr.employees` — 10 rows with PII (SSN, phone, email)
- `hr.departments` — 5 rows
- `sales.orders` — 7 rows
- `sales.products` — 8 rows
- `sales.order_items` — 12 rows

### SQL Server (port 1433)
- `hr.employees` — 5 rows with PII
- `finance.transactions` — 8 rows with financial data

### MySQL (port 3306)
- `customers` — 5 rows with contact info
- `invoices` — 7 rows with payment data

### MongoDB (port 27017)
- `events` collection — 8 documents (web analytics events)
- `sensor_readings` collection — 6 documents (IoT sensor data)

### Flat Files (SFTP + local)
- `employees.csv` — CSV with PII fields (for masking tests)
- `transactions.csv` — CSV with financial data
- `orders_fixed_width.dat` — Fixed-width format file

Files are available at:
- SFTP: `/upload/sample-data/`
- App container: `/data/sample-data/`

## Testing Scenarios

### 1. RDBMS to Local Filesystem
Create a pipeline from PostgreSQL source (port 5433) to local filesystem target.
Expected: PySpark script generated, data written as parquet to `/data/output/`.

### 2. Flat File to ADLS2 (Azurite)
Create a pipeline from SFTP source to Azure Blob (Azurite).
Expected: Python tasks script with SFTP sensor + upload function.

### 3. Database with Advanced Features
Create a pipeline from SQL Server to S3 (MinIO) with column mapping, DQ rules, and masking.
Expected: PySpark script with JDBC read, column transforms, DQ validation, and SSN/phone masking.

### 4. DAG Deployment to Airflow
Deploy a pipeline to Airflow DAG folder. Verify:
- YAML appears in Airflow UI
- `generate_dags.py` bootstrap is in dags root
- dag-factory parses the YAML and creates DAG

### 5. Flat File with Wildcard
SFTP source with wildcard pattern `/upload/sample-data/*.csv`.
Expected: Sensor checks all matching files, ingests each.

## Connection Strings for DataFlow

Use these when creating connections in the DataFlow UI:

| Platform | Host | Port | Database | User | Password |
|----------|------|------|----------|------|----------|
| PostgreSQL | postgres-source (or localhost) | 5433 | source_db | source_user | source_pass |
| SQL Server | sqlserver (or localhost) | 1433 | source_db | sa | DataFlow#2026! |
| MySQL | mysql (or localhost) | 3306 | source_db | source_user | source_pass |
| MongoDB | mongodb (or localhost) | 27017 | source_db | root | rootpass |
| SFTP | sftp (or localhost) | 2222 | — | testuser | testpass |
| S3 (MinIO) | minio (or localhost) | 9000 | — | minioadmin | minioadmin |
| Azure Blob (Azurite) | azurite (or localhost) | 10000 | — | devstoreaccount1 | (default key) |

**Note**: Use service names (e.g. `postgres-source`) when connecting from within Docker. Use `localhost` with mapped ports when connecting from the host machine.

## Airflow Configuration

Airflow comes pre-configured with:
- `dag-factory>=1.0.0` installed
- LocalExecutor (single-node, no Celery needed)
- Basic auth enabled (admin/admin)
- REST API enabled at `http://localhost:8080/api/v1/`
- DAGs folder shared with the app container at `/airflow-dags`

To configure DataFlow to deploy to this Airflow instance:
- Airflow URL: `http://airflow-webserver:8080` (from Docker) or `http://localhost:8080` (from host)
- DAG Folder path: `/opt/airflow/dags` (inside Airflow container)

## Cleanup

```bash
# Stop all services
docker compose down

# Stop and remove all data volumes
docker compose down -v
```
