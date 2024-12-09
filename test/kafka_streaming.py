from pymongo import MongoClient
from kafka import KafkaProducer
import json
import time
import logging

client = MongoClient("mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017/cdp_database?authSource=admin")
db = client["cdp_database"]
tweets_collection = db["tweets"]

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = "tweets_topic"

def send_data_to_kafka():
    cur_time = time.time()
    for tweet in tweets_collection.find():
        if time.time() > cur_time + 10:
            break
        else:
            try:
                producer.send(topic, tweet) 
                print(f"Sent tweet")
            except Exception as e:
                logging.error(f'An error occurred: {e}')
                continue
        time.sleep(0.1)

if __name__ == "__main__":
    send_data_to_kafka()
    producer.flush()
    producer.close()
