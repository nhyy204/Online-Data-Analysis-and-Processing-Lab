# 💾 Online Data Analysis and Processing Lab

This repository contains lab exercises and assignments completed as part of the Online Data Analysis and Processing (ODAP) course, as well as a capstone **Project** simulating a real-time credit card transaction pipeline.

---

## 📂 Repository Structure

```
Online-Data-Analysis-and-Processing-Lab/
│
├── Project/                    # Capstone project: real-time credit card transaction pipeline
├── Week 1 - Python/            # Lab 1: Python basics and data manipulation
├── Week 2 - Kafka/             # Lab 2: Apache Kafka producer/consumer exercises
├── Week 3 - Hadoop/            # Lab 3: Introduction to HDFS and batch data storage
├── Week 4 - Spark/             # Lab 4: Batch processing with Apache Spark
├── Week 5 - Pyspark/           # Lab 5: PySpark DataFrame API and transformations
├── Week 6 - Pyspark Streaming/ # Lab 6: Real-time stream processing with PySpark
└── .gitignore                  # Git ignore rules
```

---

## 🧩 Lab Summaries

### Week 1 - Python

- **Objective:** Practice Python fundamentals, file I/O, and data structures.
- **Content:** Reading/writing CSVs, list/dictionary operations, basic data cleaning scripts.

### Week 2 - Kafka

- **Objective:** Understand Apache Kafka architecture.
- **Content:** Implement a producer to send sample events and a consumer to read and display messages.

### Week 3 - Hadoop

- **Objective:** Learn HDFS basics.
- **Content:** Upload sample datasets to HDFS and manage HDFS directories.

### Week 4 - Spark

- **Objective:** Introduction to Apache Spark for batch processing.
- **Content:** RDD operations and Spark SQL queries.

### Week 5 - PySpark

- **Objective:** Use PySpark DataFrame API for structured data.
- **Content:** DataFrame transformations, aggregations, and parquet file I/O.

### Week 6 - PySpark Streaming

- **Objective:** Build streaming applications with Spark Structured Streaming.
- **Content:** Read from socket, perform windowed aggregations, and write streaming output.

---

## 🚀 Capstone Project: Real-Time Credit Card Transaction Pipeline

A simulation of a real-time transaction processing system for a financial company:

1. **Dataset:** CSV file of credit card transactions simulating POS swipes.
2. **Ingestion (Kafka):** Producer reads CSV lines at random 1–3 second intervals and publishes to `credit-card-transactions` topic.
3. **Processing (Spark Streaming):** Consumer reads from Kafka, filters out errors and frauds, formats date/time, converts currency to VND, and writes cleaned data to HDFS.
4. **Storage (HDFS):** Stores processed CSV batches for analytics.
5. **Visualization (Power BI):** Reads HDFS data to generate daily reports of transaction counts and amounts per merchant, grouped by day, month, and year.
6. **Scheduling (Airflow):** Daily DAG refreshes Power BI dataset to ensure up-to-date dashboards.

### Architect overview

![image](https://github.com/user-attachments/assets/e24362d4-7e69-44c7-9e47-cb8fd2683114)

### 📁 Project Folder Layout

```
Project/
├── Dataset/                   # Sample transaction CSVs
│   └── credit_card_transactions.csv
├── Kafka/                     # Simulates POS transactions and sends to Kafka
│   └── kafka_producer.py
├── Pyspark streaming/         # Spark Structured Streaming app
│   └── pyspark_streaming.py
├── Hadoop/                    # Push lastest data to powerBI
│   └── serving.py
│   └── last_push_timestamp.txt
├── Airflow/                   # Airflow DAG for dashboard refresh
│   └── schedule.py
└── reset.sh                   # Reset HDFS Checkpoints and Output Folder
└── README.md                  # Project-specific instructions (this file)
└── requirements.txt           # Python dependencies for Project
```

### 🛠️ Prerequisites

Before running any components, ensure you have the following installed and configured:

- Java JDK 11: Required by Apache Hadoop HDFS and Apache Spark (batch and streaming).

- Java JDK 17: Recommended for Apache Kafka (broker and client) and Apache Airflow.

- Apache Kafka (2.4+): For real-time messaging.

- Apache Spark (3.0+): Spark Streaming and Structured Streaming support.

- Hadoop HDFS (3.x): Distributed storage for processed data.

- Python 3.7+: For Kafka producer, Spark scripts, and serving script.

- Apache Airflow (2.x): Workflow orchestration.

- Power BI Desktop / Service: For data visualization and REST API endpoint access.

### 📦 Python Dependencies

All Python dependencies for the Capstone Project are specified in `Project/requirements.txt`. To install, run

```
pip install -r Project/requirements.txt
```

### 🛠️ Initial Setup: Start Required Services

Before running the pipeline, ensure the following services are started

1. **Start Hadoop (HDFS)**
   ```
   start-dfs.sh
   ```
2. **Reset HDFS Checkpoints and Output Folder For First Time Run**
   ```
   Project/reset.sh
   ```

### ⚙️ How to Run

Configure environment variables in Project/.env (template):

```
# Kafka settings
KAFKA_BOOTSTRAP_SERVERS=<your_kafka_bootstrap_servers>
KAFKA_TOPIC=<your_kafka_topic>
DATA_FILE_PATH=<path_to_transaction_csv>

# HDFS settings

HDFS_NAMENODE=<your_hdfs_namenode_url>
HDFS_PATH=<your_hdfs_directory_for_transactions>
HDFS_CHECKPOINT=<your_hdfs_checkpoint_directory>
HDFS_USER=<your_hdfs_username>

# Exchange rate API

EXCHANGE_RATE_API_URL=<your_exchange_rate_api_endpoint>

# Power BI settings

POWERBI_URL=<your_powerbi_push_url>
LAST_PUSH_TIMESTAMP_PATH=<path_to_local_timestamp_file>

# User environment

HOME=<your_path>

```

1. **Start Kafka** and create topic `credit-card-transactions`.
2. **Run Kafka Producer**:

   ```bash
   cd Project/Kafka
   python kafka_producer.py
   ```

3. **Submit Spark Streaming Job**:

   ```bash
   cd Project/'Pyspark Streaming'
   spark-submit pyspark_streaming.py
   ```

4. **Verify HDFS Output**: Check cleaned data files in HDFS.
5. **Push Processed Data to Power BI**:

   ```
   cd Project/Hadoop
   python serving.py
   ```

This script reads new records from HDFS since the last run and pushes them in batches to the Power BI REST endpoint

6. **Schedule Daily Refresh with Airflow**:

```

cd Project/Airflow
airflow dags trigger update_dashboard_dag

```

---

## 📝 License

This repository is for educational purposes and ODAP course exercises.
