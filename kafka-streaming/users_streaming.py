from pymongo import MongoClient
from kafka import KafkaProducer
import json
import logging
import time
client = MongoClient("mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017/cdp_database?authSource=admin")
db = client["cdp_database"]
users_collection = db["twitter_users"]

producer = KafkaProducer(
    bootstrap_servers=['broker:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "users_topic"

def send_data_to_kafka():
    for user in users_collection.find():
        try:
            producer.send(topic, user) 
            print("Sent user")
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue
    time.sleep(0.1)

if __name__ == "__main__":
    send_data_to_kafka()
    producer.flush()
    producer.close()
