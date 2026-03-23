# Banking Fraud Detection Data Platform

## Overview
End-to-end data pipeline for detecting fraudulent banking transactions.  
Technologies: Kafka, Spark Structured Streaming, PostgreSQL, Airflow, Ollama AI for fraud explanation.

## Features
- Kafka producer simulates transactions.
- Spark streaming reads transactions and identifies fraud.
- PostgreSQL stores transactions, fraud alerts, and AI-generated explanations.
- AI enrichment provides concise fraud explanations.
- Airflow DAG orchestrates the workflow.

## Project Structure
banking-fraud-detection-data-platform/
│
├─ producer/ # Kafka transaction generator
├─ spark_processor/ # Spark streaming scripts
├─ ai_analysis/ # AI enrichment scripts
├─ dags/ # Airflow DAGs
├─ README.md
├─ requirements.txt
└─ .gitignore

## Setup
1. Install Python 3.9.6 and create virtual environment.
2. Install dependencies: `pip install -r requirements.txt`.
3. Run Kafka & Zookeeper.
4. Start Airflow and DAG.
5. Run the pipeline: transactions -> Spark -> AI enrichment -> PostgreSQL.

## Output
- `transactions` table: all transactions
- `fraud_alerts` table: filtered fraudulent transactions
- `fraud_explained` table: concise AI explanations
