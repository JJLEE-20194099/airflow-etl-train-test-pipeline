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
from src.models.single_model import xgb
from src.models.single_model import lgbm
from src.models.single_model import abr
from src.models.single_model import etr
from src.models.single_model import gbr
from src.models.single_model import knr
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

HCM_CONFIG = json.load(open('/home/long/long/datn-feast/feature_repo/src/config/featureset/full_version.json'))
feature_dict = json.load(open(HCM_CONFIG['featureset_path'], 'r'))
cat_cols = feature_dict['cat_cols']
num_cols = feature_dict['num_cols']

all_cols = cat_cols + num_cols
feast_dataset_name = HCM_CONFIG["feast_dataset_name"]


@dag("train_hcm_etr_model", tags = ["train_hcm_etr_model"], schedule="*/59 * * * *", catchup=False, start_date=datetime(2024, 6, 6))
def taskflow():

    @task(task_id="train_hcm_etr_model", retries=2)
    def train():

        fs = FeatureStore(repo_path=os.getenv('FEAST_FEATURE_REPO'))
        train_data_source_name = feast_dataset_name
        full_df = fs.get_saved_dataset(name=train_data_source_name).to_df()

        train_df = full_df.iloc[:-10954]
        test_df = full_df.iloc[-10954:]

        target_feature = 'target'
        target_feature_alias = 'Price (million/m2) / 100'

        categorical_features_indices = [i for i, c in enumerate(all_cols) if c in cat_cols]

        cat_model = cat.create_model(cat_idxs = categorical_features_indices)
        lgbm_model = lgbm.create_model(cat_names = cat_cols)
        xgb_model = xgb.create_model()
        abr_model = abr.create_model()
        etr_model = etr.create_model()
        gbr_model = gbr.create_model()
        knr_model = knr.create_model()
        la_model = la.create_model()
        mlp_model = mlp.create_model()
        rf_model = rf.create_model()
        linear_model = linear.create_model()


        train_etr_model_run_id = mlflow_train_model(
            model = etr_model,
            train_df = train_df,
            test_df = test_df,
            train_data_source_name = train_data_source_name,
            experiment_name = 'hcm_etr_realestate_num_version_training',
            selected_features = all_cols,
            target_feature = target_feature,
            target_feature_alias = target_feature_alias,
            model_name = 'etr'
        )

    train()
dag = taskflow()