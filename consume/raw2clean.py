import threading
import func_timeout
import time
from kafka import KafkaProducer, KafkaConsumer
from process.batdongsan import transferBatdongsan
import json
from tqdm import tqdm

from dotenv import load_dotenv
import os
load_dotenv(override=True)

class Kafka:
    def __init__(self):
        self.kafka_host = os.getenv('KAFKA_HOST')
        self.kafka_port = os.getenv('KAFKA_PORT')
        self.producer = KafkaProducer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'])
        #self.consumer = KafkaConsumer(bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'], auto_offset_reset='earliest', enable_auto_commit=True, group_id=self.kafka_group_id,value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    def kafka_consumer(self, kafka_group_id, kafka_topic):
        """_summary_

        Args:
            kafka_group_id (_type_): group id of consumer
            kafka_topic (_type_): list topic to subscribe

        Returns:
            _type_: consumer
        """
        consumer = KafkaConsumer(
            bootstrap_servers=[f"{self.kafka_host}:{self.kafka_port}"],
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=kafka_group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            max_poll_records=10
        )
        consumer.subscribe(kafka_topic)
        return consumer

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


KafkaInstance = Kafka()
MAX_THREAD = 10


def processBatdongsan(msg, consumer):
    print("Start processing property")
    data = msg.value
    databatdongsan = transferBatdongsan(data["html_source"])
    print(databatdongsan)
    if databatdongsan != None:
        KafkaInstance.send_data(databatdongsan, "datn_batdongsan")
        consumer.commit()
        print("Done saving 1 property")

def runFunction(f, max_wait, args, consumer, default_value=None):
    try:
        return func_timeout.func_timeout(
            max_wait,
            f,
            args=(
                args,
                consumer,
            ),
        )
    except Exception as e:
        print("Thread timed out!!!")
        pass
    return default_value

def runBatdongsan():
    try:
        print("Start interval transfer batdongsan")
        consumer = KafkaInstance.kafka_consumer("raw_batdongsan", ["raw_batdongsan"])
        # threads = []
        for msg in tqdm(consumer):
            try:
                print("Processing property")
                processBatdongsan(msg, consumer)
            except Exception as e:
                print("Error")

    except Exception as error:
        print("Error on process batdongsan.com: ", error)


runBatdongsan()

