import warnings

from airflow import DAG
from airflow.decorators import task, dag
from utils.train_func import train_model_by_city_data_and_feature_version
from datetime import datetime, timedelta

from dotenv import load_dotenv
load_dotenv(override=True)
import os
import warnings
warnings.filterwarnings('ignore')

from airflow.sensors.external_task_sensor import ExternalTaskSensor


def create_dag(dag_id, schedule, tags, default_args, city, model_name):
    @dag(dag_id=dag_id, schedule=schedule, tags = tags, default_args=default_args, catchup=False)
    def taskflow():

        @task(task_id=f"train_{city}_{model_name}_model_v0", retries=0)
        def train_v0():
            train_model_by_city_data_and_feature_version(city = city, version = 0, model_name = model_name)

        @task(task_id=f"train_{city}_{model_name}_model_v1", retries=0)
        def train_v1():
            train_model_by_city_data_and_feature_version(city = city, version = 1, model_name = model_name)

        @task(task_id=f"train_{city}_{model_name}_model_v2", retries=0)
        def train_v2():
            train_model_by_city_data_and_feature_version(city = city, version = 2, model_name = model_name)

        @task(task_id=f"train_{city}_{model_name}_model_v4", retries=0)
        def train_v4():
            train_model_by_city_data_and_feature_version(city = city, version = 4, model_name = model_name)

        @task(task_id=f"train_{city}_{model_name}_model_v5", retries=0)
        def train_v5():
            train_model_by_city_data_and_feature_version(city = city, version = 5, model_name = model_name)

        wait_for_data_pipeline = ExternalTaskSensor(
            task_id='wait_for_data_pipeline',
            external_dag_id='data_pipeline',
            external_task_id='build_training_dataset',
            start_date=datetime(2024, 7, 1),
            allowed_states=['success'],
            failed_states=['failed', 'skipped'],
            mode = 'reschedule'
        )

        wait_for_data_pipeline >> [train_v0(), train_v1(), train_v2(), train_v4(), train_v5()]

    generated_dag = taskflow()


for city in ['hcm', 'hn']:
    for model_name in ['abr', 'cat', 'etr', 'gbr', 'knr', 'la', 'lgbm', 'linear', 'mlp', 'rf', 'ridge', 'xgb']:
        dag_id = f"train_{city}_{model_name}_model"
        default_args = {"owner": "airflow", "start_date": datetime(2024, 7, 1)}
        # schedule = "@daily"
        schedule = None

        globals()[dag_id] = create_dag(dag_id, schedule, [dag_id], default_args, city, model_name)





