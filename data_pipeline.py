import warnings

from airflow import DAG
from airflow.decorators import task, dag
from clean_raw_data import clean
from get_raw_data import crawl
from insert_clean_data import insert
from utils.train_func import train_model_by_city_data_and_feature_version
from datetime import datetime, timedelta
import requests

from dotenv import load_dotenv
load_dotenv(override=True)
import os
import warnings
warnings.filterwarnings('ignore')

@dag("data_pipeline", tags = ["data_pipeline"], schedule='@daily', catchup=False, start_date=datetime(2024, 6, 6))
def taskflow():

    @task(task_id="get_raw_data", retries=0)
    def crawl_data():
        crawl()

    @task(task_id="clean_raw_data", retries=0)
    def clean_data():
        clean()

    @task(task_id="insert_clean_data", retries=0)
    def insert_data():
        insert()

    @task(task_id="build_offline_batch_data", retries=0)
    def build_data():
        url = f"{os.getenv('BKPRICE_SERVER')}/build-offline-batch-data"
        payload = {}
        headers = {}
        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.json())

    t1 = crawl_data()
    t2 = clean_data()
    t3 = insert_data()
    t4 = build_data()

    t1 >> t2 >> t3 >> t4
dag = taskflow()
