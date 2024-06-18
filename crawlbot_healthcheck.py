from dotenv import load_dotenv
load_dotenv(override=True)
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import time
import requests
from datetime import datetime
import random

def make_requests(url):
    res = requests.request("GET", url)
    if res.status_code == 200:
        return res
    else:
        print(f'Error: {res.status_code}')
        print('Log: ', res.text)
        return None

crawlbot_server = os.getenv('CRAWLBOT_SERVER')
