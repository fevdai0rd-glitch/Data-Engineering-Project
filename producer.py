import time
import json
import requests
from kafka import KafkaProducer

# Connect to the Kafka container using its network name inside Docker
producer = KafkaProducer(
    bootstrap_servers=['kafka:29092'], # Inside the Docker network, we use the container name!
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

API_URL1 = "https://api.coinbase.com/api/v2/exchange-rates/?currency=BTC"
API_URL2 = "https://api.coinbase.com/v2/time"
TOPIC_NAME = "crypto_prices"

print("🚀 Starting Live Crypto Stream to Kafka... Press Ctrl+C to stop.")

try:
    while True:
        response1 = requests.get(API_URL1)
        response2 = requests.get(API_URL2)
        if response1.status_code == 200:
            raw_data1 = response1.json()
        else:
            print(f"⚠️ Exchange rate API Error: {response1.status_code}")

        if response2.status_code == 200:
            raw_data2 = response2.json()
        else:
            print(f"⚠️ Time API Error: {response2.status_code}")

        payload = {
            "timestamp": raw_data2["data"]["iso"],
            "currency": "BTC",
            "rate_usd": float(raw_data1["data"]["rates"]["USD"]),
            "rate_gbp": float(raw_data1["data"]["rates"]["GBP"])
        }
        producer.send(TOPIC_NAME, value=payload)
        print(f"📦 Sent to Kafka: {payload}")
        time.sleep(10) # Fetch data every 10 seconds
except KeyboardInterrupt:
    print("\n🛑 Stream stopped.")
finally:
    producer.flush()
