# Real-Time Industrial Anomaly Detection via WaterLog Big Data Infrastructure

![Java](https://img.shields.io/badge/Java-17.0.12-blue.svg)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange.svg)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-4.2.0-black.svg)
![ONNX Runtime](https://img.shields.io/badge/ONNX_Runtime-1.16.2-blue)
![Python](https://img.shields.io/badge/Python-3.12-yellow.svg)

## 📖 Overview
This repository contains the source code and infrastructure configuration for a **True Real-time Intrusion Detection System (IDS)** designed for critical water infrastructures. Built on the **WaterLog** dataset, the system utilizes a distributed streaming architecture leveraging **Apache Kafka** and **Apache Spark Structured Streaming**. 

To overcome the severe Java Virtual Machine (JVM) Garbage Collection (GC) overheads inherent in Apache Spark MLlib, this project introduces a novel **Decoupled Layered Pipeline**. By manually vectorizing flat Z-score math and embedding a Deep Multilayer Perceptron (MLP) via an **ONNX Runtime C++ Engine**, the system completely bypasses the JVM memory bottlenecks.

## System Architecture

<img width="586" height="144" alt="image" src="https://github.com/user-attachments/assets/e41561ba-0b52-4b72-b0ae-4ea8f2a02211" />



## ✨ Key Features & Scientific Contributions
* **Sub-second Latency & High Throughput:** Achieves a peak throughput of ~11,538.79 rows/s with an ultra-low latency of 869.70 ms.
* **JVM-ONNX Hybridization:** Executes AVX2 matrix vectorization natively on the CPU by initializing the C++ ONNX engine per-partition within Spark's `mapPartitions`, operating at the speed of lightweight linear models.
* **Perfect Parallel Alignment:** The "Golden Configuration" rigorously aligns 4 Kafka input partitions, 4 Spark shuffle partitions, and 4 Kafka output partitions directly 1-to-1 with physical CPU hardware, eliminating context-switching overhead.
* **Memory Invariance:** Strictly locked 4GB JVM Heap with the G1GC garbage collector ensures a flat physical RAM consumption profile (~4.65 GB) with zero memory leakage.

## 🛠️ Technology Stack
* **OS:** Ubuntu 24.04.4 LTS
* **Languages:** Java 17 LTS, Python 3.12, Scala 2.12.18
* **Big Data Frameworks:** Apache Spark 3.5.0, Apache Kafka 4.2.0 (Kraft Mode)
* **Deep Learning & Inference:** Keras/TensorFlow 3 (Offline Training), ONNX Runtime 1.16.2 (Online Inference)
* **Build & Containerization:** Apache Maven 3.8.7, Docker 29.1.3, Docker Compose 3.8

## 📊 Dataset
The **WaterLog** dataset simulates cyber-physical attacks across a critical water distribution network.
* **Total Rows:** 180,000 (129,890 Normal / 50,110 Attack)
* **Features:** 16 physical sensor and actuator characteristics
* **Problem Type:** Binary Classification (Normal `0.0` vs Attack `1.0`)

## 🚀 Getting Started

### Prerequisites: Docker, Java 17+, Maven 3.8+, and Python 3.12 must be installed.

### 1. Start Kafka Broker (Kraft Mode)
Ensure Docker is installed and start the Kafka cluster (ZooKeeper-less).
```bash
docker-compose up -d
```

### 2. Create Kafka Topics
Execute the following commands to create the required 4-partition topics for perfect parallel alignment:
```bash
# Input topic for raw data
sudo docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic jsonproducerp3 --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1

# Output topic for normal traffic
sudo docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic normalRealtime2 --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1

# Output topic for cyberattack anomalies
sudo docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic anormalRealtime2 --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
```

### 3. Run the Data Producer
Activate the Python virtual environment and run the high-load producer script to simulate incoming sensor traffic.
```bash
./run_load_test.sh
# or run the python script directly (configured with 2 parallel processes)
python Realtime_producer.py
```

### 4. Run the Real-Time Streaming Consumer
The project provides two processing pipelines. To launch the system with optimal JVM Heap and GC tuning, use the provided Maven commands.

**Option A: Traditional Machine Learning (Logistic Regression, Random Forest, GBT)**
```bash
MAVEN_OPTS="-Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -Xlog:gc*:file=gc_4g.log -XX:+ExitOnOutOfMemoryError --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
mvn exec:java -Dexec.mainClass="realtime_ML"
```

**Option B: Deep Learning (Hybrid Deep MLP via ONNX) - Recommended**
```bash
MAVEN_OPTS="-Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -Xlog:gc*:file=gc_4g.log -XX:+ExitOnOutOfMemoryError --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" \
mvn exec:java -Dexec.mainClass="realtime_DL"
```

## Experimental Results (Hardware constrained: 4 Cores, 4GB Heap)

| Metrics | Logistic Regression | Random Forest | GBT | Deep MLP (Proposed) |
|---|---|---|---|---|
| **Accuracy** | 96.11% | 99.15% | 99.17% | **99.46%** |
| **Throughput** | **11,565.71 rows/s** | 10,148.39 rows/s | 9,942.29 rows/s | 11,538.79 rows/s |
| **Average Latency** | **865.20 ms** | 980.38 ms | 998.67 ms | 869.70 ms |
| **Avg CPU Usage** | 36.42% | 40.14% | **35.66%** | 40.90% |
| **RAM (RSS)** | ~4,648 MB | ~4,660 MB | ~4,640 MB | ~4,657 MB |


## System Evolution Chart
<img width="915" height="490" alt="image" src="https://github.com/user-attachments/assets/6ec9ab2d-0b69-411d-85c1-62808bf4ad5a" />

The chart illustrates throughput scaling and latency evolution during infrastructure tuning across different maxOffsetsPerTrigger configurations.

*Results from cross-model evaluation under the 500,000 maxOffsetsPerTrigger and 5-second trigger interval configuration*.

## Author
Nguyễn Trường Vũ  
Information Systems Student  
University of Information Technology

---
*Note: Real-time logs are continuously appended to `metrics/stream_metrics.csv` for throughput and latency analysis.*
