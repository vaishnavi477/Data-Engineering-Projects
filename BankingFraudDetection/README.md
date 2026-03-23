# 🏦 Banking Fraud Detection Data Platform

## 📌 Overview

This project implements an **end-to-end real-time data pipeline** to simulate, process, and analyze banking transactions for fraud detection.

The system ingests streaming transaction data, applies rule-based fraud detection using distributed processing, and enriches flagged transactions with **AI-generated explanations**. The entire workflow is orchestrated using a production-style scheduling pipeline.

---

## 🧠 Problem Statement

Financial institutions process millions of transactions in real time. Detecting fraudulent activity requires:

* Low-latency data ingestion
* Scalable stream processing
* Reliable storage
* Explainable fraud detection

This project demonstrates how to build such a system using modern data engineering tools.

---

## 🏗️ Architecture

```text
Kafka Producer → Kafka Topic → Spark Structured Streaming → PostgreSQL
                                             ↓
                                      Fraud Detection
                                             ↓
                                 AI Enrichment (Ollama)
                                             ↓
                                    Airflow Orchestration
```

---

## ⚙️ Tech Stack

| Layer         | Technology                          |
| ------------- | ----------------------------------- |
| Ingestion     | Apache Kafka                        |
| Processing    | Apache Spark (Structured Streaming) |
| Storage       | PostgreSQL                          |
| Orchestration | Apache Airflow                      |
| AI Enrichment | Ollama (Llama3)                     |
| Language      | Python 3.9.6                        |

---

## ✨ Key Features

* **Real-time Data Simulation**
  Kafka producer generates streaming banking transactions.

* **Streaming Fraud Detection**
  Spark processes transactions and flags high-risk ones based on business rules.

* **Dual Data Storage**

  * `transactions` → all incoming data
  * `fraud_alerts` → flagged transactions

* **AI-Powered Explainability**
  Uses LLM (Llama3 via Ollama) to generate concise fraud explanations.

* **Workflow Orchestration**
  Airflow DAG automates:

  * Data generation
  * Stream processing
  * AI enrichment

* **Environment-Based Configuration**
  All configs managed via `.env` for portability and security.

---

## 📂 Project Structure

```
Banking Fraud Detection Data Platform/
│
├── producer/
│   └── Transaction_Generator.py
│
├── spark_processor/
│   └── spark_stream.py
│
├── ai_analysis/
│   └── fraud_ai.py
│
├── airflow/
│   └── fraud_pipeline.py
│
├── .env
├── requirements.txt
└── README.md
```

---

## ⚡ Setup Instructions

### 1️⃣ Prerequisites

* Python **3.9.6**
* Apache Kafka & Zookeeper
* Apache Spark
* PostgreSQL
* Apache Airflow
* Ollama (for AI)

---

### 2️⃣ Create Virtual Environment

```bash
python3.9 -m venv venv
source venv/bin/activate
```

---

### 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

⚠️ Airflow installation (recommended):

```bash
pip install "apache-airflow==2.9.3" \
--constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.9.txt
```

---

### 4️⃣ Configure Environment Variables

Create a `.env` file:

```env
POSTGRES_DB=banking
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=transactions

OLLAMA_API_URL=http://localhost:11434/api/generate
OLLAMA_MODEL=llama3

# Project paths
PROJECT_PATH=/your_path/Banking Fraud Detection Data Platform
PYTHON_PATH=/your_path/airflow_venv/bin/activate
SPARK_PATH=spark-submit
OLLAMA_PATH=/opt/homebrew/bin/ollama
```

---

### 5️⃣ Start Required Services

```bash
# Start Zookeeper
brew services start zookeeper

# Start Kafka
brew services start kafka

# Start PostgreSQL
brew services start postgresql

# Start Airflow
One terminal - airflow scheduler
Second terminal - airflow webserver --port 8081
```

---

### 6️⃣ Before running the pipeline, create the Kafka topic:

```bash
kafka-topics.sh --create \
--topic transactions \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1
```

### 7️⃣ Run the Pipeline

Trigger the Airflow DAG:

```
fraud_pipeline

Execution flow:

Transaction Generator → Spark Processing → AI Enrichment → Data Storage
```
---

## 📊 Output Tables

### 🔹 `transactions`

Stores all incoming transactions.

### 🔹 `fraud_alerts`

Contains transactions flagged as potentially fraudulent.

### 🔹 `fraud_explained`

Stores AI-generated explanations for fraud cases.

---

## 🧪 Example Output

| transaction_id | explanation                                  |
| -------------- | -------------------------------------------- |
| 4263           | Large amount in foreign country              |
| 4678           | Unusual high-value international transaction |

---

## 🚀 Key Learnings

* Built a **streaming data pipeline using Kafka + Spark**
* Implemented **real-time fraud detection logic**
* Integrated **LLM-based explainability into data workflows**
* Designed **Airflow DAG for orchestration**
* Applied **environment-driven configuration**

---

## 🔐 Notes

* `.env` is excluded from version control
* All credentials should be managed securely

---

## 📌 Future Improvements

* Replace rule-based fraud detection with ML model
* Add dashboard (e.g., Streamlit or Tableau)
* Implement data quality checks
* Deploy using Docker

---

## 👤 Author

Vaishnavi Patil

---

## ⭐ If you found this useful

Feel free to star the repo and connect!
