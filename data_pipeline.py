import warnings

from airflow import DAG
from airflow.decorators import task, dag
from clean_raw_data import clean
from get_raw_data import crawl
from insert_clean_data import insert
from utils.train_func import train_model_by_city_data_and_feature_version
from datetime import datetime, timedelta
import requests
import json

from dotenv import load_dotenv
load_dotenv(override=True)
import os
import warnings
warnings.filterwarnings('ignore')

@dag("data_pipeline", tags = ["data_pipeline"], schedule='@daily', catchup=False, start_date=datetime(2024, 6, 6))
def taskflow():

    # @task(task_id="get_raw_data", retries=0)
    # def crawl_data():
    #     crawl()

    # @task(task_id="clean_raw_data", retries=0)
    # def clean_data():
    #     clean()

    # @task(task_id="insert_clean_data", retries=0)
    # def insert_data():
    #     insert()

    @task(task_id="crawl_clean_insert", retries=0)
    def crawl_clean_insert_data():
        print("Start to delete data from queue")
        os.system('sh delete_data.sh')
        print("Start to run datapipeline")
        os.system('sh datapipeline_oneshot.sh')
        print("End to delete data from queue, run datapipeline")



    @task(task_id="build_offline_batch_data", retries=0)
    def build_data():
        url = f"{os.getenv('BKPRICE_SERVER')}/build-offline-batch-data"
        payload = {}
        headers = {}
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.json())

    @task(task_id="extract_feature", retries=0)
    def extract_feature():

        print("Start to create feature view")
        url = f"{os.getenv('FEAST_SERVER')}/get_store"
        payload = json.dumps({
            "path": "feature_repo/"
        })
        headers = {
        'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.json())
        print("End to create feature view")


    @task(task_id="build_training_dataset", retries=0)
    def create_dataset():

        print("Start to create feature dataset")
        url = f"{os.getenv('FEAST_SERVER')}/build-training-dataset"
        payload = {}
        headers = {}
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.json())
        print("End to create feature dataset")


    t1 = crawl_clean_insert_data()
    t2 = build_data()
    t3 = extract_feature()
    t4 = create_dataset()

    t1 >> t2 >> t3 >> t4
dag = taskflow()
