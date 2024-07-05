import warnings

from airflow import DAG
from airflow.decorators import task, dag
from clean_raw_data import clean
from clients.discord_client import DiscordClient
from get_raw_data import crawl
from insert_clean_data import insert
from utils.train_func import train_model_by_city_data_and_feature_version
from datetime import datetime, timedelta
import requests
import json
from airflow import XComArg
from airflow.api.client.local_client import Client
import time

from dotenv import load_dotenv
load_dotenv(override=True)
import os
import warnings
warnings.filterwarnings('ignore')

mlops_webhook_url = os.getenv('MLOPS_WEBHOOK_URL')
mlops_client = DiscordClient(url = mlops_webhook_url, bot_name="DATN - MLOPS BOT")

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

        exp_id =  f'data_pipeline_{datetime.now()}'
        exp_id = "".join([char for char in exp_id if char.isalnum()])

        mlops_client.webhook_push_sucess_noti(title = f"START - EXPERIMENT - {exp_id}", message = f"EXPERIMENT - {exp_id}")

        print("Start to delete data from queue")
        os.system('sh delete_data.sh')
        print("Start to run datapipeline")
        os.system('sh datapipeline_oneshot.sh')
        print("End to delete data from queue, run datapipeline")

        mlops_client.webhook_push_sucess_noti(title = f"DONE DATAPIPELINE - EXPERIMENT - {exp_id}", message = f"EXPERIMENT - {exp_id}")


        return exp_id



    @task(task_id="build_offline_batch_data", retries=0)
    def build_data(exp_id):
        url = f"{os.getenv('BKPRICE_SERVER')}/build-offline-batch-data"
        payload = json.dumps({
            "exp_id": exp_id
        })
        headers = {
        'Content-Type': 'application/json'
        }

        mlops_client.webhook_push_sucess_noti(title = f"START BUILD OFFLINE BATCH DATA - EXPERIMENT - {exp_id}", message = f"EXPERIMENT - {exp_id}")

        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.json())
        result =  response.json()
        result = {**result, "exp_id": exp_id}

        mlops_client.webhook_push_sucess_noti(title = f"DONE BUILD OFFLINE BATCH DATA - EXPERIMENT - {exp_id}", message = f"EXPERIMENT - {exp_id}")

        return result

    @task(task_id="extract_feature", retries=0)
    def extract_feature(dataset_metadata):

        print("Start to create feature view")
        url = f"{os.getenv('FEAST_SERVER')}/get_store"
        payload = json.dumps({
            "path": "feature_repo/"
        })
        headers = {
        'Content-Type': 'application/json'
        }

        exp_id = dataset_metadata['exp_id']

        mlops_client.webhook_push_sucess_noti(title = f"START EXTRACT FEATURE - EXPERIMENT - {exp_id}", message = f"EXPERIMENT - {exp_id}")

        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.json())
        print("End to create feature view")

        mlops_client.webhook_push_sucess_noti(title = f"DONE EXTRACT FEATURE - EXPERIMENT - {exp_id}", message = f"EXPERIMENT - {exp_id}")

        return dataset_metadata



    @task(task_id="build_training_dataset", retries=0)
    def create_dataset(dataset_metadata):

        print("Start to create feature dataset")
        url = f"{os.getenv('FEAST_SERVER')}/build-training-dataset"
        payload = {}
        headers = {}

        exp_id = dataset_metadata['exp_id']
        mlops_client.webhook_push_sucess_noti(title = f"START BUILD TRAINING DATASET - EXPERIMENT - {exp_id}", message = f"EXPERIMENT - {exp_id}")

        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.json())
        print("End to create feature dataset")
        print(dataset_metadata)

        mlops_client.webhook_push_sucess_noti(title = f"DONE BUILD TRAINING DATASET - EXPERIMENT - {exp_id}", message = f"EXPERIMENT - {exp_id}")

        return dataset_metadata

    @task(task_id="trigger_training_ai_model", retries=0)
    def trigger_training_ai_model(dataset_metadata):
        c = Client(None, None)

        exp_id = dataset_metadata['exp_id']
        mlops_client.webhook_push_sucess_noti(title = f"START TRIGGER TRAINING MODEL - EXPERIMENT - {exp_id}", message = f"EXPERIMENT - {exp_id}")

        for city in ['hcm', 'hn']:
            for model_name in ['abr', 'cat', 'etr', 'gbr', 'knr', 'la', 'lgbm', 'linear', 'mlp', 'rf', 'ridge', 'xgb']:
                dag_id = f"train_{city}_{model_name}_model"
                run_id = f'{dag_id}_{datetime.now()}'
                run_id = "".join([char for char in run_id if char.isalnum()])
                c.trigger_dag(dag_id=dag_id, run_id=run_id, conf={})

        mlops_client.webhook_push_sucess_noti(title = f"DONE TRIGGER TRAINING MODEL - EXPERIMENT - {exp_id}", message = f"EXPERIMENT - {exp_id}")




    t1 = crawl_clean_insert_data()
    t2 = build_data(t1)
    t3 = extract_feature(t2)
    t4 = create_dataset(t3)
    t5 = trigger_training_ai_model(t4)


    t1 >> t2 >> t3 >> t4 >> t5
dag = taskflow()
