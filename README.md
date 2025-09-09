# 🏆 Olympic Data Engineering Pipeline

## 📋 Project Overview

Complete end-to-end data engineering solution for Olympic athlete data processing, featuring both **batch processing** and **real-time streaming** pipelines with multi-layer data lake architecture.

### 🎯 Key Features
- **Dual Pipeline Architecture**: Batch (Data Lake) + Streaming (Real-time)
- **Medallion Architecture**: Landing → Bronze → Silver → Gold layers
- **Modern Stack**: Spark, Kafka, Airflow, MySQL, Docker
- **Production Ready**: Error handling, monitoring, testing, documentation

---

## 🏗️ Architecture

### System Architecture
```
┌──────────────────────────────────────────────────────────────────┐
│                        BATCH PIPELINE                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐      │
│  │ Landing  │→ │  Bronze  │→ │  Silver  │→ │     Gold     │      │
│  │  (CSV)   │  │(Parquet) │  │  (Clean) │  │ (Analytics)  │      │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────┘      │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                      STREAMING PIPELINE                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐      │
│  │  MySQL   │→ │  Kafka   │→ │  Spark   │→ │ Kafka/MySQL  │      │
│  │ (Source) │  │ (Stream) │  │(Process) │  │   (Sink)     │      │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────┘      │
└──────────────────────────────────────────────────────────────────┘
```

### Container Architecture
```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Airflow    │  │    Spark     │  │    Kafka     │
│  Webserver   │  │   Master     │  │   Broker     │
│  Scheduler   │  │   Worker     │  │  Zookeeper   │
└──────────────┘  └──────────────┘  └──────────────┘
        ↓                 ↓                 ↓
┌──────────────────────────────────────────────────┐
│         Shared Data Lake Volume                  │
│      /tmp/data_lake (Landing→Gold)               │
└──────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Make (optional, for convenience commands)

### Installation & Setup

```bash
# 1. Clone repository
git clone https://github.com/DenysRudenko2/goit-de-fp.git
cd goit-de-fp

# 2. Configure environment variables
cp .env.example .env
# Edit .env file with your credentials:
# - KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_BOOTSTRAP_SERVERS
# - MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
# - USER_NAME (your username for topic naming)

# 3. Start services using Make
make start-streaming    # Start all services
make trigger-dag        # Run batch pipeline
make status            # Check services
make stop              # Stop all services
```

### Environment Variables
All sensitive configuration is managed through environment variables in the `.env` file:

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `USER_NAME` | ✅ | Your username for topic naming | `rudenko` |
| `KAFKA_BOOTSTRAP_SERVERS` | ✅ | Kafka broker addresses | `77.81.230.104:9092` |
| `KAFKA_USERNAME` | ✅ | Kafka SASL username | `admin` |
| `KAFKA_PASSWORD` | ✅ | Kafka SASL password | `your_password` |
| `MYSQL_HOST` | ✅ | MySQL server hostname | `217.61.57.46` |
| `MYSQL_USER` | ✅ | MySQL username | `neo_data_admin` |
| `MYSQL_PASSWORD` | ✅ | MySQL password | `your_mysql_password` |
| `MYSQL_DATABASE` | ✅ | MySQL database name | `olympic_dataset` |
| `FTP_URL` | ❌ | Data source FTP URL | `https://ftp.goit.study/neoversity/` |
| `DATA_LAKE_PATH` | ❌ | Data lake storage path | `/tmp/data_lake` |
| `SPARK_MASTER` | ❌ | Spark master URL | `local[*]` |

**⚠️ Important**: The application will fail with clear error messages if required environment variables are not set.

### Access Points
- **Airflow UI**: http://localhost:8090 (airflow/airflow)
- **Spark UI**: http://localhost:8080

---

## 📊 Batch Pipeline (Data Lake)

### Overview
Multi-hop data lake processing Olympic athlete historical data through medallion architecture.

### Pipeline Stages

#### 1️⃣ **Landing to Bronze**
- Downloads CSV files from FTP server
- Converts to Parquet with compression
- Adds metadata (timestamp, source)
- **Input**: athlete_bio.csv (55MB), athlete_event_results.csv (32MB)
- **Output**: 155,861 bio + 316,834 event records

#### 2️⃣ **Bronze to Silver**
- Data cleaning and normalization
- Special character removal
- Deduplication (~1,200 duplicates removed)
- Null value handling
- **Output**: Clean, deduplicated data

#### 3️⃣ **Silver to Gold**
- Feature engineering (BMI, age calculation)
- Joins bio and event data
- Creates analytics aggregations
- **Outputs**:
  - `athlete_features`: 326,204 enriched records
  - `sport_statistics`: 192 sport/gender combinations

### Running Batch Pipeline

```bash
# Option 1: Via Airflow DAG
make trigger-dag

# Option 2: Direct execution
python dags/landing_to_bronze.py
python dags/bronze_to_silver.py
python dags/silver_to_gold.py
```

---

## 🔄 Streaming Pipeline (Real-time)

### Overview
Real-time processing of athlete data from MySQL through Kafka to analytics outputs.

### Pipeline Components

#### 📖 **Step 1-2: MySQL Source**
- Reads athlete_bio with height/weight filtering
- Validates numeric data quality
- Filters invalid/null values

#### 📤 **Step 3: MySQL → Kafka**
- Streams athlete_event_results to Kafka
- JSON serialization with schema
- Topic: `rudenko_athlete_event_results`

#### 🔄 **Step 4-5: Stream Processing**
- Kafka → Spark Structured Streaming
- Joins with athlete_bio data
- Calculates aggregations by sport/medal/country
- Windowed computations with watermarks

#### 📊 **Step 6: Dual Output (forEachBatch)**
- **6a**: Enriched data → Kafka topic
- **6b**: Aggregations → MySQL table
- Output topic: `rudenko_enriched_athlete_avg`

### Running Streaming Pipeline

```bash
# Start streaming infrastructure
make start-streaming

# Trigger streaming DAG
docker compose exec airflow-webserver \
  airflow dags trigger rudenko_streaming_pipeline

# Or run components manually
python 01_create_kafka_topics.py
python 02_mysql_to_kafka.py
python 03_streaming_processor.py
```

---

## 📁 Project Structure

```
final-project/
├── 📊 Batch Processing
│   ├── dags/
│   │   ├── landing_to_bronze.py    # CSV → Parquet conversion
│   │   ├── bronze_to_silver.py     # Data cleaning
│   │   ├── silver_to_gold.py       # Feature engineering
│   │   └── project_solution.py     # Batch DAG
│   │
├── 🔄 Streaming Processing
│   ├── 01_create_kafka_topics.py   # Topic setup
│   ├── 02_mysql_to_kafka.py        # Data ingestion
│   ├── 03_streaming_processor.py   # Stream processing
│   ├── 04_test_kafka_read.py       # Testing utility
│   └── 05_kafka_table_display.py   # Data visualization
│   │
├── 🐳 Infrastructure
│   ├── docker-compose-streaming.yaml # All services configuration
│   ├── Dockerfile-streaming        # Custom Airflow + Spark image
│   └── Makefile                    # All commands for managing the pipeline
│   │
└── 📚 Documentation
    └── README.md                   # This file
```

---

## 🔧 Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Orchestration** | Apache Airflow | DAG scheduling and monitoring |
| **Processing** | Apache Spark | Batch and stream processing |
| **Streaming** | Apache Kafka | Message broker for real-time data |
| **Storage** | Parquet/MySQL | Columnar files and relational DB |
| **Containerization** | Docker/Compose | Service isolation and deployment |
| **Languages** | Python/PySpark | Data processing logic |

---

## 📈 Data Quality & Validation

### Quality Metrics
- **Null Analysis**: Height/Weight (27.9%), Age (98.8%), Medals (85.9%)
- **Duplicates**: ~1,200 removed in Silver layer
- **Anomalies**: 2,553 BMI outliers, 3,432 age outliers
- **Completeness**: 326,204 valid joined records from 472,695 total

### Validation Framework
```python
# Schema validation
assert "athlete_id" in df.columns
assert df.schema["height"].dataType == FloatType()

# Business rules
assert df.filter(col("bmi") > 100).count() < threshold
assert df.filter(col("age") < 10).count() == 0

# Data quality
null_percentage = df.filter(col("weight").isNull()).count() / df.count()
assert null_percentage < 0.3
```

---

## 🛠️ Operations

### Makefile Commands

```bash
# Service Management
make start           # Start batch infrastructure
make start-streaming # Start streaming infrastructure
make stop           # Stop all services
make restart        # Restart all services

# Pipeline Operations
make trigger-dag    # Run batch pipeline
make test-kafka     # Test Kafka connectivity
make show-data      # Display sample data

# Maintenance
make fix-permissions # Fix volume permissions
make clean          # Remove all data/volumes
make logs           # Show container logs
make status         # Show service status
```

### Monitoring

```bash
# Check pipeline status
docker compose exec airflow-webserver \
  airflow dags state rudenko_data_lake_pipeline

# View streaming metrics
docker compose logs -f spark-master

# Monitor Kafka topics
docker compose exec kafka \
  kafka-console-consumer.sh --topic rudenko_enriched_athlete_avg
```

---

## 🎯 Performance Metrics

### Batch Pipeline
- **Landing → Bronze**: 30-45 seconds
- **Bronze → Silver**: 20-30 seconds
- **Silver → Gold**: 40-50 seconds
- **Total**: ~90-120 seconds

### Streaming Pipeline
- **Latency**: <1 second end-to-end
- **Throughput**: ~10,000 records/second
- **Checkpoint Interval**: 10 seconds

---

## 🐛 Troubleshooting

### Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| **Permission Denied** | Run `make fix-permissions` |
| **Kafka Connection Failed** | Check network and credentials in config |
| **Spark OOM** | Increase Docker memory allocation |
| **DAG Not Found** | Wait for Airflow to scan DAGs (~30s) |
| **Port Conflicts** | Change ports in docker-compose.yaml |

### Debug Commands
```bash
# Check container logs
docker compose logs [service-name]

# Access container shell
docker compose exec airflow-webserver bash

# View Spark UI
open http://localhost:8080

# Test data lake access
docker compose exec airflow-webserver ls -la /tmp/data_lake/
```

---

## ✅ Project Achievements

### Batch Pipeline
- ✅ Multi-hop medallion architecture
- ✅ Automated data ingestion from FTP
- ✅ Data quality and deduplication
- ✅ Feature engineering (BMI, age)
- ✅ Sport analytics aggregations

### Streaming Pipeline
- ✅ Real-time MySQL → Kafka ingestion
- ✅ Spark Structured Streaming processing
- ✅ Stream-static joins
- ✅ Windowed aggregations
- ✅ Dual output (Kafka + MySQL)

### Infrastructure
- ✅ Full Docker containerization
- ✅ Airflow orchestration
- ✅ Shared volume architecture
- ✅ Production error handling
- ✅ Comprehensive testing suite

---

## 📚 Data Sources

- **FTP Server**: https://ftp.goit.study/neoversity/
- **MySQL Database**: 217.61.57.46:3306/neo_data
- **Kafka Broker**: 77.81.230.104:9092

---

## 👥 Author

**Denys Rudenko**
- GitHub: [@DenysRudenko2](https://github.com/DenysRudenko2)
- Project: [goit-de-fp](https://github.com/DenysRudenko2/goit-de-fp)

---

## 📄 License

This project is part of the GoIT Data Engineering course final project.

---

**🎉 All project requirements successfully implemented!**