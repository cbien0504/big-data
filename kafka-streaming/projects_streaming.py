from pymongo import MongoClient
from kafka import KafkaProducer
import json
import logging

client = MongoClient("mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017/cdp_database?authSource=admin")
db = client["cdp_database"]
projects_collection = db["projects_social_media"]

producer = KafkaProducer(
    bootstrap_servers=['broker:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "projects_topic"

def send_data_to_kafka():
    for project in projects_collection.find():
        try:
            producer.send(topic, project) 
            print("Sent project")
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue
    time.sleep(0.1)

if __name__ == "__main__":
    send_data_to_kafka()
    producer.flush()
    producer.close()
