import pendulum
import os
import pandas as pd
import json
import requests
from datetime import datetime
import boto3
from io import StringIO
from sqlalchemy import create_engine
from feast import FeatureStore

import mlflow
import tempfile
import warnings

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow import DAG
from airflow.decorators import task, dag
from src.models.single_model import cat
from src.models.single_model import lgbm
from dotenv import load_dotenv

from src.train.single_model import mlflow_train_model
load_dotenv(override=True)
import os

import warnings
warnings.filterwarnings('ignore')


@dag("train_model", tags = ["train_model"], schedule="*/1 * * * *", catchup=False, start_date=datetime(2024, 6, 6))
def taskflow():

    @task(task_id="train", retries=2)
    def train():
        cat_model = cat.create_model()
        lgbm_model = lgbm.create_model()

        fs = FeatureStore(repo_path=os.getenv('FEAST_FEATURE_REPO'))
        train_data_source_name = "realestate_dataset_1"
        full_df = fs.get_saved_dataset(name=train_data_source_name).to_df()

        train_df = full_df.iloc[:100]
        test_df = full_df.iloc[100:200]

        target_feature = 'target'
        target_feature_alias = 'Price (million/m2)'


        train_cat_model_run_id = mlflow_train_model(
            model = cat_model,
            train_df = train_df,
            test_df = test_df,
            train_data_source_name = train_data_source_name,
            experiment_name = 'cat_realestate_test_training_phrase_1',
            selected_features = ['used_area', 'num_of_bedroom', 'num_of_bathroom', 'district'],
            target_feature = target_feature,
            target_feature_alias = target_feature_alias,
            model_name = 'catboost'
        )

        train_lgbm_model_run_id = mlflow_train_model(
            model = lgbm_model,
            train_df = train_df,
            test_df = test_df,
            train_data_source_name = train_data_source_name,
            experiment_name = 'lgbm_realestate_test_training_phrase_1',
            selected_features = ['used_area', 'num_of_bedroom', 'num_of_bathroom', 'district'],
            target_feature = target_feature,
            target_feature_alias = target_feature_alias,
            model_name = 'lgbm'
        )

    train()
dag = taskflow()
