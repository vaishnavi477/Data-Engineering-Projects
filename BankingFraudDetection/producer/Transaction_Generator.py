import json
import time
import random
import os
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv

# ----------------------------------------
# Load environment variables
# ----------------------------------------
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
NUM_TRANSACTIONS = int(os.getenv("NUM_TRANSACTIONS", 20))

# ----------------------------------------
# Kafka producer
# ----------------------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

countries = ["US", "UK", "IN", "CA", "DE"]

# ----------------------------------------
# Generate transactions
# ----------------------------------------
for _ in range(NUM_TRANSACTIONS):
    amount = random.randint(100, 5000)
    transaction = {
        "transaction_id": random.randint(1000, 9999),
        "user_id": random.randint(1, 1000),
        "amount": amount,
        "country": random.choice(countries),
        "risk_level": "HIGH" if amount > 3000 else "LOW",
        "timestamp": datetime.now().isoformat()
    }

    producer.send(KAFKA_TOPIC, transaction)
    print(transaction)

    time.sleep(0.1)

# ----------------------------------------
# Flush messages
# ----------------------------------------
producer.flush()
print(f"\n✅ Sent {NUM_TRANSACTIONS} transactions to Kafka topic '{KAFKA_TOPIC}'"):wq!
