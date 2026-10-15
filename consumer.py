import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# Connect to the local MongoDB container using its network name
mongo_client = MongoClient("mongodb://mongodb:27017")
db = mongo_client["crypto_data"]         # Creates a database named crypto_data
collection = db["bitcoin_prices"]        # Creates a collection named bitcoin_prices

# Connect to Kafka to listen for messages
consumer = KafkaConsumer(
    'crypto_prices',
    bootstrap_servers=['kafka:29092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("🎧 Consumer is listening for Kafka messages to store in MongoDB...")

for message in consumer:
    # Decode the JSON payload from Kafka
    data_packet = message.value
    
    # Insert it directly into MongoDB as a NoSQL document
    inserted_id = collection.insert_one(data_packet).inserted_id
    print(f"💾 Saved to MongoDB (ID: {inserted_id}): {data_packet}")
