from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
from dotenv import load_dotenv

# ----------------------------------------
# Load environment variables
# ----------------------------------------
load_dotenv()

PROJECT_PATH = os.getenv("PROJECT_PATH")
PYTHON_PATH = os.getenv("PYTHON_PATH")
SPARK_PATH = os.getenv("SPARK_PATH")
OLLAMA_PATH = os.getenv("OLLAMA_PATH")

dag = DAG(
    'fraud_detection_pipeline',
    start_date=datetime(2026, 3, 18),
    catchup=False
)

# ----------------------------------------
# Generate transactions
# ----------------------------------------
generate = BashOperator(
    task_id='generate_transactions',
    bash_command=f'{PYTHON_PATH} "{PROJECT_PATH}/producer/Transaction_Generator.py"',
    dag=dag
)

# ----------------------------------------
# Process with Spark
# ----------------------------------------
process = BashOperator(
    task_id='spark_process',
    bash_command=f'''
    {SPARK_PATH} \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.postgresql:postgresql:42.7.3 \
    "{PROJECT_PATH}/spark_processor/spark_stream.py"
    ''',
    dag=dag
)

# ----------------------------------------
# Start Ollama
# ----------------------------------------
start_ollama = BashOperator(
    task_id='start_ollama',
    bash_command=f'''
    {OLLAMA_PATH} serve > /tmp/ollama.log 2>&1 &
    echo $! > /tmp/ollama_pid.txt

    echo "Waiting for Ollama..."

    for i in {{1..15}}
    do
        sleep 2
        curl -s http://localhost:11434 > /dev/null && echo "Ollama ready" && exit 0
    done

    echo "Ollama failed"
    exit 1
    ''',
    dag=dag
)

# ----------------------------------------
# AI Enrichment
# ----------------------------------------
ai_enrich = BashOperator(
    task_id='fraud_ai_enrichment',
    bash_command=f'{PYTHON_PATH} "{PROJECT_PATH}/ai_analysis/fraud_ai.py"',
    dag=dag
)

# ----------------------------------------
# Stop Ollama
# ----------------------------------------
stop_ollama = BashOperator(
    task_id='stop_ollama',
    bash_command='''
    if [ -f /tmp/ollama_pid.txt ]; then
        echo "Stopping Ollama..."
        kill $(cat /tmp/ollama_pid.txt) 2>/dev/null || true
        rm /tmp/ollama_pid.txt
        echo "Ollama stopped."
    else
        echo "No PID found"
    fi
    ''',
    dag=dag
)

# ----------------------------------------
# DAG Order
# ----------------------------------------
generate >> process >> start_ollama >> ai_enrich >> stop_ollama
