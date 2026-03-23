import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# ------------------------------
# Kafka producer
# ------------------------------
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

countries = ["US", "UK", "IN", "CA", "DE"]

# ------------------------------
# Generate exactly 20 transactions
# ------------------------------
for _ in range(20):
    amount = random.randint(100, 5000)
    transaction = {
        "transaction_id": random.randint(1000, 9999),
        "user_id": random.randint(1, 1000),
        "amount": amount,
        "country": random.choice(countries),
        "risk_level": "HIGH" if amount > 3000 else "LOW",
        "timestamp": datetime.now().isoformat()
    }

    producer.send("transactions", transaction)
    print(transaction)

    # small delay to avoid overwhelming Kafka
    time.sleep(0.1)

# flush producer to ensure all messages are sent
producer.flush()

