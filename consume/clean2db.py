from kafka import KafkaConsumer
import pymongo
import json
from dotenv import load_dotenv
import os
import math
load_dotenv(override=True)

from pymongo import InsertOne
from tqdm import tqdm

def nan_2_none(obj):
    if isinstance(obj, dict):
        return {k:nan_2_none(v) for k,v in obj.items()}
    elif isinstance(obj, list):
        return [nan_2_none(v) for v in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    return obj



kafka_broker = os.getenv('KAFKA_BROKER')
kafka_topic = ['datn_batdongsan']




def consume_messages():

    consumer = KafkaConsumer(bootstrap_servers=kafka_broker, auto_offset_reset='earliest', group_id = 'datn_clean_to_db', enable_auto_commit=True,value_deserializer=lambda x: json.loads(x.decode('utf-8')), consumer_timeout_ms = 1000)
    consumer.subscribe(kafka_topic)

    update_data_list = []
    updated_ids = []
    operations = []

    connection_str = os.getenv('REALESTATE_DB')
    __client = pymongo.MongoClient(connection_str)

    database = 'realestate'
    __database = __client[database]

    for message in consumer:
        message_data = message.value

        record = nan_2_none(message_data)

        operations.append(
            InsertOne(record)
        )

        print("Check")

    collection = __database["realestate_listing"]
    # print(collection')
    if len(operations):
        collection.bulk_write(operations,ordered=False)

if __name__ == "__main__":
    consume_messages()
