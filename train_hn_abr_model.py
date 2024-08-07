# import warnings

# from airflow import DAG
# from airflow.decorators import task, dag
# from utils.train_func import train_model_by_city_data_and_feature_version
# from datetime import datetime, timedelta


# from dotenv import load_dotenv
# load_dotenv(override=True)
# import os
# import warnings
# warnings.filterwarnings('ignore')

# @dag("train_hn_abr_model", tags = ["train_hn_abr_model"], schedule='0 10,19 * * *', catchup=False, start_date=datetime(2024, 6, 6))
# def taskflow():

#     @task(task_id="train_hn_abr_model_v0", retries=0)
#     def train_v0():
#         train_model_by_city_data_and_feature_version(city = 'hn', version = 0, model_name = 'abr')

#     @task(task_id="train_hn_abr_model_v1", retries=2)
#     def train_v1():
#         train_model_by_city_data_and_feature_version(city = 'hn', version = 1, model_name = 'abr')

#     @task(task_id="train_hn_abr_model_v2", retries=0)
#     def train_v2():
#         train_model_by_city_data_and_feature_version(city = 'hn', version = 2, model_name = 'abr')

#     @task(task_id="train_hn_abr_model_v4", retries=0)
#     def train_v4():
#         train_model_by_city_data_and_feature_version(city = 'hn', version = 4, model_name = 'abr')

#     @task(task_id="train_hn_abr_model_v5", retries=0)
#     def train_v5():
#         train_model_by_city_data_and_feature_version(city = 'hn', version = 5, model_name = 'abr')

#     [train_v0(), train_v1(), train_v2(), train_v4(), train_v5()]
# dag = taskflow()
