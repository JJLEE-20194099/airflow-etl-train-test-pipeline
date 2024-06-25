import pendulum
import os
import pandas as pd
import json
import requests
from datetime import datetime, timedelta
import boto3
from io import StringIO
from sqlalchemy import create_engine
from feast import FeatureStore

import mlflow
import tempfile
from src.helpers.reliable_tool import get_trustworthy_dataset
import warnings

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow import DAG
from airflow.decorators import task, dag
from src.models.single_model import cat
from src.models.single_model import xgb
from src.models.single_model import lgbm
from src.models.single_model import abr
from src.models.single_model import etr
from src.models.single_model import gbr
from src.models.single_model import la
from src.models.single_model import la
from src.models.single_model import mlp
from src.models.single_model import rf
from src.models.single_model import linear



from dotenv import load_dotenv

from src.train.single_model import mlflow_train_model
load_dotenv(override=True)
import os

import warnings
warnings.filterwarnings('ignore')

city = 'hcm'
version = 0
model_name = 'linear'
pretrained_file = f'/home/long/airflow/dags/models/{city}/{model_name}/v{version}/model.joblib'


FS = json.load(open(f'/home/long/long/datn-feast/feature_repo/src/config/featureset/update_data/demo1/{city}_v{version}.json'))
feature_dict = json.load(open(FS['featureset_path'], 'r'))
cat_cols = feature_dict['cat_cols']
num_cols = feature_dict['num_cols']

all_cols = cat_cols + num_cols
feast_dataset_name = FS["feast_dataset_name"]

EXP = os.getenv('EXP')

@dag("train_hcm_linear_model", tags = ["train_hcm_linear_model"], schedule='0 10,19 * * *', catchup=False, start_date=datetime(2024, 6, 6))
def taskflow():

    @task(task_id="train_hcm_linear_model", retries=0)
    def train():

        fs = FeatureStore(repo_path=os.getenv('FEAST_FEATURE_REPO'))
        train_data_source_name = feast_dataset_name
        train_df = fs.get_saved_dataset(name=train_data_source_name).to_df()
        test_df = get_trustworthy_dataset(city, version)

        print('TRAIN_SHAPE:', train_df.shape)
        print('TEST_SHAPE:', test_df.shape)


        target_feature = 'target'
        target_feature_alias = 'Price (million/m2)'
        categorical_features_indices = [i for i, c in enumerate(all_cols) if c in cat_cols]

        linear_model = linear.create_model(pretrained_file = pretrained_file)

        train_linear_model_run_id = mlflow_train_model(
            model = linear_model,
            train_df = train_df,
            test_df = test_df,
            train_data_source_name = train_data_source_name,
            experiment_name = f'hcm_linear_realestate_{EXP}',
            selected_features = all_cols,
            target_feature = target_feature,
            target_feature_alias = target_feature_alias,
            model_name = 'linear'
        )

    train()
dag = taskflow()

