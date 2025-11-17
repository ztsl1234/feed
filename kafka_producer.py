from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_trade():
    return {
        "trade_id": f"T{random.randint(100,999)}",
        "counterparty": random.choice(["ABC","XYZ","MNO","PQR"]),
        "notional": random.randint(100000, 5000000),
        "currency": random.choice(["USD", "EUR", "JPY", "SGD"]),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

topic = "risk_trades"

print(f"Producing messages to {topic}...")

while True:
    msg = generate_trade()
    producer.send(topic, msg)
    producer.flush()
    print("Sent:", msg)
    time.sleep(1)
