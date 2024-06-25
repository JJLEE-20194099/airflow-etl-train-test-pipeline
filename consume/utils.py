from kafka import KafkaProducer, KafkaConsumer

from dotenv import load_dotenv
import os
load_dotenv(override=True)

import redis
import json

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

class Redis:
    def __init__(self):
        self.redis_host = os.getenv("REDIS_HOST")
        self.redis_port = os.getenv("REDIS_PORT")
        self.redis_db = os.getenv("REDIS_DB")
        self.pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, db=self.redis_db)
        self.redis = redis.Redis(connection_pool=self.pool, charset="utf-8", decode_responses=True)

    def check_id_exist(self, id, set_name):
        """_summary_

        Args:
            id (_type_): id of data ( generate by url or id of data in website)
            set_name (_type_): name of set in redis

        Returns:
            _type_: False if id not exist and no add to set, True if id exist
        """
        return self.redis.sismember(set_name, id)

    def add_id_to_set(self, id, set_name):
        """_summary_

        Args:
            id (_type_): id of data ( generate by url or id of data in website)
            set_name (_type_): name of set in redis

        Returns:
            _type_: False if add fail, True if add success
        """
        return self.redis.sadd(set_name, id)

class Kafka:
    def __init__(self):
        self.kafka_host = os.getenv('KAFKA_HOST')
        self.kafka_port = os.getenv('KAFKA_PORT')
        self.producer = KafkaProducer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'])
        #self.consumer = KafkaConsumer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'], auto_offset_reset='earliest', enable_auto_commit=True, group_id=self.kafka_group_id,value_deserializer=lambda x: json.loads(x.decode('utf-8')))


    def send_data(self, data,kafka_topic):
        """_summary_

        Args:
            data (_type_): data to send to kafka
            kafka_topic (_type_): topic to send data

        Returns:
            _type_: False if send fail, True if send success
        """
        status = self.producer.send(kafka_topic, value = json.dumps(data).encode('utf-8'))
        self.producer.flush()
        if status.is_done == True:
            return True
        else:
            return False


    def create_consumer_and_subscribe(self, kafka_group_id, kafka_topic):
        """_summary_

        Args:
            kafka_group_id (_type_): group id of consumer
            kafka_topic (_type_): list topic to subscribe

        Returns:
            _type_ : consumer
        """
        consumer = KafkaConsumer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'], auto_offset_reset='earliest', enable_auto_commit=True, group_id=kafka_group_id,value_deserializer=lambda x: x.decode('utf-8'))
        consumer.subscribe(kafka_topic)
        return consumer
