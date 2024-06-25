

# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import threading
# import time
# import requests
# import os
# from dotenv import load_dotenv

# import requests
# from bs4 import BeautifulSoup
# import time
# import pymongo
# from pymongo import InsertOne
# from dotenv import load_dotenv
# from datetime import datetime
# import os
# load_dotenv(override=True)

# from seleniumbase import SB
# from consume.utils import  Redis, Kafka

# crawlbot_server = os.getenv('CRAWLBOT_SERVER')

# class Mogi:
#     def crawl_url(self,page,proxy=None):
#         url = requests.request("GET", f'{crawlbot_server}/mogi/crawl_url?page={page}')
#         return url.json()

#     def crawl_data_by_url(self,url,proxy=None):
#         url = requests.request("GET", f'{crawlbot_server}/mogi/crawl_data_by_url?url={url}')
#         return url.json()


# class Batdongsan:
#     def crawl_url(self,page,proxy=None):
#         if proxy is not None:
#             url = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_url?page={page}&proxy={proxy}')
#         else:
#             url = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_url?page={page}')
#         if url.status_code == 200:
#             return list(url.json())
#         else:
#             print('Error when crawl url from batdongsan.com.vn, status code: ', url.status_code)
#             return []

#     def crawl_data_by_url(self,url,proxy=None):
#         if proxy is not None:
#             data = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_data_by_url?url={url}&proxy={proxy}')
#         else:
#             data = requests.request("GET", f'{crawlbot_server}/batdongsan/crawl_data_by_url?url={url}')
#         if data.status_code == 200:
#             return data.json()
#         else:
#             print('Error when crawl data by url from batdongsan.com.vn, status code: ', data.status_code)
#             return None

# class Muaban:
#     def crawl_id(self,offset,proxy=None):
#         url = requests.request("GET", f'{crawlbot_server}/muaban/crawl_id?offset={offset}')
#         return url.json()

#     def crawl_data_by_id(self, id, proxy=None):
#         url = requests.request("GET", f'{crawlbot_server}/muaban/crawl_data_by_id?id={id}')
#         return url.json()

# class Meeyland:
#     def crawl_data_by_page(self, page, proxy=None):
#         url = requests.request("GET", f'{crawlbot_server}/meeyland/crawl_id?page={page}')
#         return url.json()


# def crawl_batdongsan_by_url(url):
#     data = Batdongsan().crawl_data_by_url(url)
#     if data is not None:
#         if Kafka().send_data(data,'raw_batdongsan') == True:
#             Redis().add_id_to_set(url, 'raw_batdongsan')

#     return data

# def crawl_muaban_by_id(id):
#     if Redis().check_id_exist(id, 'raw_muaban'):
#         print("Existed Crawled Id")
#         return
#     data = Muaban().crawl_data_by_id(id)
#     if data is not None:
#         if Kafka().send_data(data,'raw_muaban') == True:
#             Redis().add_id_to_set(id, 'raw_muaban')
#     else:
#         print('crawl fail : ', id)

# connection_str = os.getenv('REALESTATE_DB')
# __client = pymongo.MongoClient(connection_str)

# database = 'realestate'
# __database = __client[database]

# collection = __database["realestate_url_pool"]


# def crawl_meeyland_by_page(page):
#     data = Meeyland().crawl_data_by_page(page)
#     if data is not None:
#         operations = []
#         for item in data:
#             if Redis().check_id_exist(item['_id'], 'raw_meeyland'):
#                 continue
#             if Kafka().send_data(item,'raw_meeyland') == True:
#                 Redis().add_id_to_set(item['_id'], 'raw_meeyland')
#                 operations.append(
#                     InsertOne({
#                         "crawl_at": datetime.now(),
#                         "url": item['_id'],
#                         "source": "meeyland"
#                     })
#                 )
#                 print("Insert 1 record ok")
#         if len(operations):
#             collection.bulk_write(operations,ordered=False)

# def crawl_meeyland():
#     list_data = []
#     for page in range(1, 50):
#         data = crawl_meeyland_by_page(page)
#         time.sleep(5)

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2021, 5, 17),
#     'retries': 0
# }
# dag = DAG('get_raw_data', default_args=default_args, schedule_interval='0 10,19 * * *', catchup=False)

# import threading
# import func_timeout
# import time
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# from kafka import KafkaProducer, KafkaConsumer
# from utils.meeyland_util import transferMeeyland
# import json
# from tqdm import tqdm
# from consume.utils import Redis
# from dotenv import load_dotenv
# import os
# load_dotenv(override=True)

# class Kafka:
#     def __init__(self, broker_id):
#         self.broker_id = broker_id
#         self.kafka_host = os.getenv('KAFKA_HOST')
#         self.kafka_port = os.getenv(f'KAFKA_PORT_{broker_id}')
#         self.producer = KafkaProducer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'])
#         #self.consumer = KafkaConsumer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'], auto_offset_reset='earliest', enable_auto_commit=True, group_id=self.kafka_group_id,value_deserializer=lambda x: json.loads(x.decode('utf-8')))

#     def kafka_consumer(self, kafka_group_id, kafka_topic):
#         """_summary_

#         Args:
#             kafka_group_id (_type_): group id of consumer
#             kafka_topic (_type_): list topic to subscribe

#         Returns:
#             _type_: consumer
#         """
#         consumer = KafkaConsumer(
#             bootstrap_servers=[f"{self.kafka_host}:{self.kafka_port}"],
#             auto_offset_reset="earliest",
#             enable_auto_commit=False,
#             group_id=kafka_group_id,
#             value_deserializer=lambda x: json.loads(x.decode("utf-8")),
#             max_poll_records=10
#         )
#         consumer.subscribe(kafka_topic)
#         return consumer

#     def send_data(self, data,kafka_topic):
#         """_summary_

#         Args:
#             data (_type_): data to send to kafka
#             kafka_topic (_type_): topic to send data

#         Returns:
#             _type_: False if send fail, True if send success
#         """
#         status = self.producer.send(kafka_topic, value = json.dumps(data).encode('utf-8'))
#         self.producer.flush()
#         if status.is_done == True:
#             return True
#         else:
#             return False


#     def create_consumer_and_subscribe(self, kafka_group_id, kafka_topic):
#         """_summary_

#         Args:
#             kafka_group_id (_type_): group id of consumer
#             kafka_topic (_type_): list topic to subscribe

#         Returns:
#             _type_ : consumer
#         """
#         consumer = KafkaConsumer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'], auto_offset_reset='earliest', enable_auto_commit=True, group_id=kafka_group_id,value_deserializer=lambda x: x.decode('utf-8'))
#         consumer.subscribe(kafka_topic)
#         return consumer


# KafkaInstance = Kafka(broker_id = 0)
# MAX_THREAD = 10

# with open('streets.json', encoding='utf-8') as f:
#    streets = json.load(f)


# locationql = [f'{item["STREET"].lower()}, {item["WARD"].lower()}, {item["DISTRICT"].lower()}' for item in tqdm(streets)]

# def processMeeyland(msg):
#     data = msg.value
#     dataMeeyland = transferMeeyland(data)
#     if dataMeeyland != None:
#         print("Process New Message Ok")
#         KafkaInstance.send_data(dataMeeyland, "datn_meeyland")


# def clean_meeyland():
#     consumer = KafkaInstance.kafka_consumer("raw_meeyland", ["raw_meeyland"])
#     for msg in tqdm(consumer):

#         if Redis().check_id_exist(f'meeyland_offset_{msg.offset}', 'meeyland_clean_rawdata'):
#             print("Ignore Processed Messages")
#             continue
#         Redis().add_id_to_set(f'meeyland_offset_{msg.offset}', 'meeyland_clean_rawdata')
#         processMeeyland(msg)


# from kafka import KafkaConsumer
# import pymongo
# import json
# from dotenv import load_dotenv
# from datetime import datetime, timedelta
# from airflow.operators.python_operator import PythonOperator
# from airflow import DAG
# import os
# import math

# from schema.preprocess.fillna import nan_2_none
# load_dotenv(override=True)
# from consume.utils import  Redis

# from pymongo import InsertOne
# from tqdm import tqdm


# kafka_broker = os.getenv('KAFKA_BROKER')
# kafka_topic = ['datn_meeyland']

# connection_str = os.getenv('REALESTATE_DB')
# __client = pymongo.MongoClient(connection_str)

# database = 'realestate'
# __database = __client[database]

# def consume_messages():

#     consumer = KafkaConsumer(bootstrap_servers=kafka_broker, auto_offset_reset='earliest', group_id = 'datn_clean_to_db', enable_auto_commit=True,value_deserializer=lambda x: json.loads(x.decode('utf-8')), consumer_timeout_ms = 1000)
#     consumer.subscribe(kafka_topic)

#     update_data_list = []
#     updated_ids = []
#     operations = []

#     for message in consumer:

#         if Redis().check_id_exist(f'meeyland_offset_{message.offset}', 'meeyland_insert_db'):
#             continue

#         message_data = message.value

#         Redis().add_id_to_set(f'meeyland_offset_{message.offset}', 'meeyland_insert_db')

#         record = nan_2_none(message_data)

#         operations.append(
#             InsertOne(record)
#         )

#     print("Len:", len(operations))

#     collection = __database["realestate_listing"]
#     # print(collection')
#     if len(operations):
#         collection.bulk_write(operations,ordered=False)

#     # => Trigger training AI Model


# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2021, 5, 17),
#     'retries': 0
# }
# dag = DAG('ETL', default_args=default_args, schedule_interval='0 10,19 * * *', catchup=False)

# crawl_meeyland = PythonOperator(task_id='get_raw_data', python_callable=crawl_meeyland, dag=dag)
# clean_meeyland = PythonOperator(task_id='clean_meeyland', python_callable=clean_meeyland, dag=dag)
# consume_messages = PythonOperator(task_id='insert_clean_data', python_callable=consume_messages, dag=dag)

# [crawl_meeyland]
# [clean_meeyland]
# [consume_messages]