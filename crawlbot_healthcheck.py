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

def healthcheck_batdongsan():
    page = random.randint(1, 100)
    res_url = make_requests(f'{crawlbot_server}/batdongsan/crawl_url?page={page}')
    if len(res_url.json()) == 20:
        print('PASS HEALTHCHECK CRAWL PAGE')
    url = random.choice(res_url.json())
    res_data = make_requests(f'{crawlbot_server}/batdongsan/crawl_data_by_url?url={url}')
    if 'Thông tin mô tả' in res_data.json()['html_source']:
        print('PASS HEALTHCHECK CRAWL DATA')
    else:
        print('Healthcheck batdongsan fail')
        print('Log: ', res_data.text)
        time.sleep(10)

healthcheck_batdongsan()