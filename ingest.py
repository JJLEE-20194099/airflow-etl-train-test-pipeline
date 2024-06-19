import pendulum
import os
import pandas as pd
import json
import requests
from datetime import datetime, timedelta
import boto3
from io import StringIO


from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow import DAG
from airflow.decorators import task, dag
from airflow import settings
from airflow.models import Connection

from utils import MAPPING_NAMES, TRANSFORM_FUNCTIONS

from sqlalchemy import create_engine

from dotenv import load_dotenv
load_dotenv(override=True)
import os


@dag("ingest", tags = ["ingest_data"], schedule="*/5 * * * *", catchup=False, start_date=datetime(2024, 6, 6))
def taskflow():

    @task(task_id="extract", retries=2,execution_timeout=timedelta(hours=24))
    def extract():


        s3 = boto3.resource('s3',
            endpoint_url=os.getenv('MINIO_SERVER'),
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_ACCESS_KEY'),
            aws_session_token=None,
            verify=False)
        obj = s3.Object(bucket_name='train', key='train_auto.csv').get()
        csvstring = obj['Body'].read().decode("utf-8")
        # s3.Object(bucket_name='train', key='train_auto.csv').delete()
        print(csvstring)
        return csvstring

    @task(task_id="transform", retries=2,execution_timeout=timedelta(hours=24))
    def tranform_and_load(csv):
        # if we expect really large file: arrow, spark, ..
        df = pd.read_csv(StringIO(csv))
        df = df.rename(columns=MAPPING_NAMES).transform(TRANSFORM_FUNCTIONS)
        engine = create_engine(os.getenv('POSTGRES_SERVER'))
        df.to_sql('drivers', engine, if_exists='append', index=False)

    extracted = extract()
    to_load = tranform_and_load(extracted)
    extracted >> to_load
    pass

dag = taskflow()
