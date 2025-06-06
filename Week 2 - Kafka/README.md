# 📡 Kafka-based Network Monitoring System

This project includes several Kafka-based Python programs used for real-time data processing and event monitoring.

## ⚙️ System Requirements

- **Python**: 3.6 or above
- **Apache Kafka**
- **Python Library**: `confluent-kafka`
  - Install using:
    ```bash
    pip install confluent-kafka
    ```

## 🔧 Installation & Configuration

### 1. Install and Start Kafka

Start the required Kafka services:

- **Zookeeper**
  ```bash
  .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
  ```
- **Kafka Server**
  ```bash
  .\bin\windows\kafka-server-start.bat .\config\server.properties
  ```

Create Kafka topic:

- **Topic: network-status with at least 1 partition**
  ```bash
  kafka-topics.sh --create --topic network-status --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
  ```
- **Topic: app-logs with at least 1 partition**
  ```bash
  kafka-topics.sh --create --topic app-logs --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
  ```
- **Topic: event with 3 partitions**
  ```bash
  kafka-topics.sh --create --topic event --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
  ```

### 2. Network Configuration

Producers and consumers connect to Kafka via `localhost:9092`
Ensure firewalls/routers/switches allow traffic on port `9092`

Check Kafka status:

- Kafka online:
  ```bash
  netstat -tuln | grep 9092
  ```
- Kafka offline:
  If can not find port 9092, verify that both Zookeeper and Kafka server are running

### 3. Running the Programs

- Exercise:
  Producer:
  ```bash
  python producer_baitap.py
  ```
  Consumer:
  ```bash
  python consumer_baitap.py
  ```
- Exercise 1:
  Producer:
  ```bash
  python producer_bai1.py
  ```
  Consumer:
  ```bash
  python consumer_bai1.py
  ```
- Exercise 2:
  Producer:
  ```bash
  python producer_bai2.py
  ```
  Consumer:
  ```bash
  python consumer_bai2.py
  ```

⚠️ Notes
Make sure Kafka server and Zookeeper are running before launching producer/consumer scripts.

To reset consumer offset:

```bash
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group-id> --reset-offsets --to-earliest --execute --topic <topic>
```

Check output logs for confirmation:

- error.log (Exercise 1)
- event_stats.log (Exercise 2)
