# 🚀 Overview

This repository showcases end-to-end data engineering projects covering real-time streaming, batch processing, data modeling, data quality, and workflow orchestration. 
The projects demonstrate production-grade design patterns including:
  - Scalable data pipelines (batch + streaming)
  - Workflow orchestration
  - Data quality and validation
  - Secure and compliant data handling
  - Integration of modern tools across the data stack

# 🏗️ Projects Included
## 🏦 Banking Fraud Detection Data Platform (Real-Time)

Overview:
A real-time streaming pipeline that ingests banking transactions, detects fraud, and generates AI-powered explanations.

Architecture:
Kafka → Spark Structured Streaming → PostgreSQL → Airflow → LLM (Ollama)

Key Features:

  - Real-time transaction ingestion using Kafka
  - Fraud detection using Spark Structured Streaming
  - AI-based explainability using Llama3
  - Workflow orchestration using Airflow
  - Scalable and fault-tolerant design

Tech Stack:
Kafka, Spark, Python, PostgreSQL, Airflow, Ollama

## 🏥 Healthcare Data Engineering Pipeline (Batch + Analytics)

Overview:
A HIPAA-compliant data pipeline that processes healthcare data from raw ingestion to analytics-ready datasets.

Architecture:
Raw Data → De-identification → PostgreSQL → dbt → Data Quality → Prefect

Key Features:

  - HIPAA Safe Harbor de-identification
  - Layered data architecture (raw → staging → marts → curated)
  - Data transformation using dbt
  - Data validation using dbt tests and Great Expectations
  - Workflow orchestration using Prefect

Tech Stack:
Python, PostgreSQL, dbt, Great Expectations, Prefect
