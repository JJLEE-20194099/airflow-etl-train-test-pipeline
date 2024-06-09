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
from airflow import settings
from airflow.models import Connection
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
    PostgreSQLOfflineStore
)

from src.models.single_model.cat import create_model
from utils.plot import plot_correlation_matrix_and_save, plot_prediction_error, plot_qq, plot_residuals, plot_time_series
from dotenv import load_dotenv
load_dotenv(override=True)
import os

import warnings
warnings.filterwarnings('ignore')


@dag("train_catboost_model", tags = ["train_catboost_model"], schedule="*/1 * * * *", catchup=False, start_date=datetime(2024, 6, 6))
def taskflow():

    @task(task_id="train", retries=2)
    def train():
        mlflow.set_tracking_uri(os.getenv('MLFLOW_SERVER'))
        mlflow.sklearn.autolog()

        fs = FeatureStore(repo_path=os.getenv('FEAST_FEATURE_REPO'))
        # training_df = PostgreSQLOfflineStore.pull_latest_from_table_or_query(
        #     config=fs.config,
        #     data_source=fs.get_data_source('drivers_source'),
        #     join_key_columns=[fs.get_entity('driver').join_key],
        #     feature_name_columns=[f.name for f in fs.get_feature_view('drivers').features],
        #     timestamp_field=fs.get_data_source('drivers_source').timestamp_field,
        #     created_timestamp_column=None,
        #     start_date=datetime(2023, 1, 1),
        #     end_date=datetime.now(),
        # ).to_df()

        training_df = fs.get_saved_dataset(name="realestate_dataset_1").to_df()

        print(training_df)
        clf = create_model()

        experiment_name = 'cat_realestate_test_training_phrase_1'
        existing_exp = mlflow.get_experiment_by_name(experiment_name)
        if not existing_exp:
            experiment_id = mlflow.create_experiment(experiment_name)
            print("Create Experiment Successfully")
        else:
            experiment_id = existing_exp.experiment_id

        timestamp = datetime.now().isoformat().split(".")[0].replace(":", ".")
        print(training_df)
        selected_features = ['used_area', 'num_of_bedroom', 'num_of_bathroom', 'district']

        training_df = training_df.sample(frac = 1)
        X_train, y_train = training_df[selected_features].iloc[:10], training_df['target'][:10]
        X_test, y_test = training_df[selected_features].iloc[30:100], training_df['target'][30:100]

        plot_correlation_matrix_and_save(training_df[selected_features + ['target']].iloc[30:200])
        with mlflow.start_run(experiment_id=experiment_id, run_name=timestamp) as run:
            clf.fit(X_train, y_train)
            cv_results = clf.cv_results_
            best_index = clf.best_index_
            for score_name in [score for score in cv_results if "mean_test" in score]:
                mlflow.log_metric(score_name, cv_results[score_name][best_index])
                mlflow.log_metric(score_name.replace("mean","std", 1), cv_results[score_name.replace("mean","std", 1)][best_index])

            tempdir = tempfile.TemporaryDirectory().name
            os.mkdir(tempdir)
            print(tempdir)
            filename = f"test_catboost-{timestamp}-cv_results.csv"
            csv = os.path.join(tempdir, filename)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                pd.DataFrame(cv_results).to_csv(csv, index=False)

            mlflow.log_artifact(csv, "cv_results")

            y_pred = clf.predict(X_test)

            fig1 = plot_time_series(training_df, x_col = 'event_timestamp', y_col = 'target', y_label_name = 'Price (million/m2)')
            fig5 = plot_residuals(y_test, y_pred)
            fig7 = plot_prediction_error(y_test, y_pred)
            fig8 = plot_qq(y_test, y_pred)

            mlflow.log_figure(fig1, "time_series_price.png")
            mlflow.log_figure(fig5, "residuals_plot.png")
            mlflow.log_figure(fig7, "prediction_errors.png")
            mlflow.log_figure(fig8, "qq_plot.png")

            mlflow.log_artifact("/tmp/corr_plot.png")


            # MLflow already log best estimator, it is needed if we want different env
            # mlflow.sklearn.log_model(clf.best_estimator_, 'RandomForest', conda_env={
			# 	'name': 'mlflow-env',
			# 	'channels': ['defaults'],
			# 	'dependencies': [
			# 		'python=3.8.10', {'pip': ['scikit-learn==1.2.1','pandas==1.5.3']}
			# 	]
            # })

    train()
dag = taskflow()
