from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

dag = DAG(
    'fraud_detection_pipeline',
    start_date=datetime(2026, 3, 18),
    # schedule_interval='@hourly',
    catchup=False
)

# 1️⃣ Generate transactions
generate = BashOperator(
    task_id='generate_transactions',
    bash_command='/Users/nikitapatel/airflow_venv/bin/python3 /Users/nikitapatel/Desktop/Data\ Projects/Banking\ Fraud\ Detection\ Data\ Platform/producer/Transaction_Generator.py',
    dag=dag
)

# 2️⃣ Process with Spark
process = BashOperator(
    task_id='spark_process',
    bash_command='''
    spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.postgresql:postgresql:42.7.3 \
    /Users/nikitapatel/Desktop/Data\ Projects/Banking\ Fraud\ Detection\ Data\ Platform/spark_processor/spark_stream.py
    ''',
    dag=dag
)

# 3️⃣ Start Ollama server (background)
start_ollama = BashOperator(
    task_id='start_ollama',
    bash_command='''
    /opt/homebrew/bin/ollama serve > /tmp/ollama.log 2>&1 &
    echo $! > /tmp/ollama_pid.txt

    echo "Waiting for Ollama to be ready..."

    for i in {1..15}
    do
        sleep 2
        curl -s http://localhost:11434 > /dev/null && echo "Ollama is ready!" && exit 0
    done

    echo "Ollama failed to start"
    exit 1
    ''',
    dag=dag
)

# 4️⃣ Run AI enrichment
ai_enrich = BashOperator(
    task_id='fraud_ai_enrichment',
    bash_command='/Users/nikitapatel/airflow_venv/bin/python3 /Users/nikitapatel/Desktop/Data\ Projects/Banking\ Fraud\ Detection\ Data\ Platform//ai_analysis/fraud_ai.py',
    dag=dag
)

# 5️⃣ Stop Ollama server
stop_ollama = BashOperator(
    task_id='stop_ollama',
    bash_command='''
    if [ -f /tmp/ollama_pid.txt ]; then
        echo "Stopping Ollama..."
        kill $(cat /tmp/ollama_pid.txt) 2>/dev/null || true
        rm /tmp/ollama_pid.txt
        echo "Ollama stopped."
    else
        echo "No Ollama PID found, skipping."
    fi
    ''',
    dag=dag
)

# Define DAG order
generate >> process >> start_ollama >> ai_enrich >> stop_ollama

