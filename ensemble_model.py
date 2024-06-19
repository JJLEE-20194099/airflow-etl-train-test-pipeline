import mlflow
import numpy as np
import pandas as pd
from src.helpers.mlflow_tool import get_best_model, get_feature_set_by_uri
from src.models.single_model.rid import create_model
from src.train.single_model import mlflow_train_model

from airflow import DAG
from airflow.decorators import task, dag

from datetime import datetime

from dotenv import load_dotenv

load_dotenv(override=True)
import os

import warnings
warnings.filterwarnings('ignore')


@dag(dag_id="ensemble_model", tags = ["ensemble_model"], schedule="*/1 * * * *", catchup=False, start_date=datetime(2024, 6, 6))
def taskflow():

    @task(task_id="ensemble_model", retries=2)
    def ensemble_model():

        experiment_name_list = [
            'cat_realestate_test_training_phrase_1',
            'lgbm_realestate_test_training_phrase_1',
            'xgb_realestate_test_training_phrase_1',
            'abr_realestate_test_training_phrase_1',
            'etr_realestate_test_training_phrase_1',
            'gbr_realestate_test_training_phrase_1',
            'knr_realestate_test_training_phrase_1',
            'la_realestate_test_training_phrase_1',
            'linear_realestate_test_training_phrase_1',
            'mlp_realestate_test_training_phrase_1',
            'rf_realestate_test_training_phrase_1'
        ]

        full_df = pd.read_parquet('/home/long/long/datn-feast/data/process_v1/process_data_7.csv')

        train_df = full_df.iloc[:-100]
        test_df = full_df.iloc[-100:]
        target_feature = 'target'
        target_feature_alias = 'target'

        oof_train_arr = []
        oof_test_arr = []


        for experiment_name in experiment_name_list:
            model, uri = get_best_model(experiment_name)
            trained_features = get_feature_set_by_uri(uri)

            filter_train_df = train_df[trained_features + [target_feature]]
            filter_test_df = test_df[trained_features + [target_feature]]

            X_train = filter_train_df[trained_features]
            X_test = filter_test_df[trained_features]


            y_train_pred = np.expand_dims(model.predict(X_train), axis = -1)
            y_test_pred = np.expand_dims(model.predict(X_test), axis = -1)

            oof_train_arr.append(y_train_pred)
            oof_test_arr.append(y_test_pred)

        oof_train_arr = np.concatenate(oof_train_arr, axis = -1)
        oof_test_arr = np.concatenate(oof_test_arr, axis = -1)

        selected_features = list(range(oof_train_arr.shape[1]))
        oof_train_df = pd.DataFrame(oof_train_arr, columns = selected_features)
        oof_test_df = pd.DataFrame(oof_test_arr, columns = selected_features)


        oof_train_df[target_feature] = train_df[target_feature]
        oof_test_df[target_feature] = test_df[target_feature]

        oof_train_df['event_timestamp'] = train_df['event_timestamp']
        oof_test_df['event_timestamp'] = test_df['event_timestamp']


        ridge_model = create_model()

        train_data_source_name = "realestate_dataset_1"
        train_ridge_model_run_id = mlflow_train_model(
                    model = ridge_model,
                    train_df = oof_train_df,
                    test_df = oof_test_df,
                    train_data_source_name = train_data_source_name,
                    experiment_name = 'ridge_realestate_test_training_phrase_1',
                    selected_features = selected_features,
                    target_feature = target_feature,
                    target_feature_alias = target_feature_alias,
                    model_name = 'ridge'
                )

    ensemble_model()
dag = taskflow()








