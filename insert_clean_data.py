from kafka import KafkaConsumer
import pymongo
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from kafka import TopicPartition
from airflow import DAG
import os
import math

from schema.preprocess.fillna import nan_2_none
load_dotenv(override=True)
from consume.utils import  Redis

from pymongo import InsertOne
from tqdm import tqdm


broker_id = 0
port = os.getenv(f"KAFKA_PORT_{broker_id}")
host = os.getenv("KAFKA_HOST")
kafka_broker = f'{host}:{port}'
kafka_topic = ['datn_meeyland']
kafka_group_id = 'datn_meeyland'
connection_str = os.getenv('REALESTATE_DB')
__client = pymongo.MongoClient(connection_str)

database = 'realestate'
__database = __client[database]

def insert():


    consumer = KafkaConsumer(
            bootstrap_servers=['localhost:9092', 'localhost:9093', 'localhost:9094'],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=kafka_group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            max_poll_records=10
        )
    consumer.assign([TopicPartition(topic, 0) for topic in kafka_topic])

    update_data_list = []
    updated_ids = []
    operations = []

    collection = __database["realestate_listing"]

    n_tries = 0
    while True:
        msg_pack = consumer.poll(timeout_ms=120000)
        cnt = 0
        for tp, messages in msg_pack.items():
            for message in messages:
                message_data = message.value

                hash_str = hash(message_data["propertyBasicInfo"]["description"]["value"])

                if Redis().check_id_exist(f'meeyland_offset_{tp.partition}_{hash_str}', 'meeyland_insert_db'):
                    continue

                Redis().add_id_to_set(f'meeyland_offset_{tp.partition}_{hash_str}', 'meeyland_insert_db')
                record = nan_2_none(message_data)
                operations.append(
                    InsertOne(record)
                )
                print("Insert 1 ok")
                cnt += 1

                if len(operations) >= 20:
                    collection.bulk_write(operations,ordered=False)
                    print(f"Insert batch size - {len(operations)} clean realestates to database")
                    operations = []
        if cnt == 0:
            n_tries += 1

        # if n_tries >= 10:
        #     break

    if len(operations):
        collection.bulk_write(operations,ordered=False)

    return n_tries



# print(insert())