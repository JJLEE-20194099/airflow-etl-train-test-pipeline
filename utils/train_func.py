from dotenv import load_dotenv

from src.train.single_model import mlflow_train_model
from utils.load_model import init
load_dotenv(override=True)
import os
import json

from feast import FeatureStore
from src.helpers.reliable_tool import get_trustworthy_dataset

from dotenv import load_dotenv
load_dotenv(override=True)
import asyncio
import cronitor.celery
from kombu import Exchange, Queue
from datetime import datetime, timedelta
from celery import Celery

cronitor.api_key = os.getenv("CRONITOR_KEY")
cronitor.environment = os.getenv("ENV_NAME")

app = Celery()
celeryconfig = {}
celeryconfig['BROKER_URL'] = os.getenv("REDIS_BROKER")
celeryconfig['CELERY_RESULT_BACKEND'] = os.getenv("REDIS_BROKER")

queue_list = []

for model_name in ['cat',
    'lgbm',
    'xgb'
]:
    for city in ['hn', 'hcm']:
        queue_name = f'{city}_{model_name}'
        queue_list.append(
            Queue(queue_name, Exchange(queue_name), routing_key=queue_name,
                queue_arguments={'x-max-priority': 10})
        )
celeryconfig['CELERY_QUEUES'] = queue_list

celeryconfig['CELERY_ACKS_LATE'] = True
celeryconfig['CELERYD_PREFETCH_MULTIPLIER'] = 1
app.config_from_object(celeryconfig)

cronitor.celery.initialize(app)

@app.task
def train_model_by_city_data_and_feature_version(city = 'hcm', version = 0, model_name = 'abr'):

    pretrained_file = f'/home/long/airflow/dags/models/{city}/{model_name}/v{version}/model.joblib'


    FS = json.load(open(f'/home/long/long/datn-feast/feature_repo/src/config/featureset/update_data/demo1/{city}_v{version}.json'))
    feature_dict = json.load(open(FS['featureset_path'], 'r'))
    cat_cols = feature_dict['cat_cols']
    num_cols = feature_dict['num_cols']

    all_cols = cat_cols + num_cols
    feast_dataset_name = FS["feast_dataset_name"]
    EXP = os.getenv('EXP')

    fs = FeatureStore(repo_path=os.getenv('FEAST_FEATURE_REPO'))
    train_data_source_name = feast_dataset_name
    train_df = fs.get_saved_dataset(name=train_data_source_name).to_df()
    test_df = get_trustworthy_dataset(city, version)

    print('TRAIN_SHAPE:', train_df.shape)
    print('TEST_SHAPE:', test_df.shape)


    target_feature = 'target'
    target_feature_alias = 'Price (million/m2)'

    categorical_features_indices = [i for i, c in enumerate(all_cols) if c in cat_cols]

    model = init(model_name, pretrained_file, categorical_features_indices)

    train_model_run_id = mlflow_train_model(
        model = model,
        train_df = train_df,
        test_df = test_df,
        train_data_source_name = train_data_source_name,
        experiment_name = f'{city}_{model_name}_realestate_{EXP}',
        selected_features = all_cols,
        target_feature = target_feature,
        target_feature_alias = target_feature_alias,
        model_name = model_name
    )