import pendulum
import os
import pandas as pd
from datetime import datetime, timedelta
from feast import FeatureStore

import mlflow
import tempfile
import warnings

from airflow import DAG
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
    PostgreSQLOfflineStore
)

from src.helpers.mlflow_tool import create_experiment
from src.helpers.training import train_test_split_by_col
from utils.plot import plot_correlation_matrix_and_save, plot_prediction_error, plot_qq, plot_residuals, plot_time_series
from dotenv import load_dotenv
load_dotenv(override=True)
import os

import warnings
warnings.filterwarnings('ignore')

def mlflow_train_model(
        model,
        train_df,
        test_df,
        train_data_source_name = 'realestate_dataset_1',
        experiment_name = 'cat_realestate_test_training_phrase_1',
        selected_features = ['used_area', 'num_of_bedroom', 'num_of_bathroom', 'district'],
        target_feature = 'target',
        target_feature_alias = 'Price (million/m2)',
        model_name = 'catboost'
    ):

    mlflow.set_tracking_uri(os.getenv('MLFLOW_SERVER'))
    mlflow.sklearn.autolog()

    fs = FeatureStore(repo_path=os.getenv('FEAST_FEATURE_REPO'))
    experiment_id = create_experiment(experiment_name)

    print("Start to calculating correlation map")
    corr_path = f"/tmp/{model_name}_corr_plot.png"
    plot_correlation_matrix_and_save(train_df[selected_features + [target_feature]], path = corr_path)

    timestamp = datetime.now().isoformat().split(".")[0].replace(":", ".")

    print("Start to training")
    with mlflow.start_run(experiment_id=experiment_id, run_name=timestamp) as run:

        X_train, X_test, y_train, y_test = train_test_split_by_col(train_df = train_df, test_df = test_df, X_cols = selected_features, y_col = target_feature)
        print(X_train)
        model.fit(X_train, y_train)
        cv_results = model.cv_results_
        best_index = model.best_index_
        for score_name in [score for score in cv_results if "mean_test" in score]:
            mlflow.log_metric(score_name, cv_results[score_name][best_index])
            mlflow.log_metric(score_name.replace("mean","std", 1), cv_results[score_name.replace("mean","std", 1)][best_index])

        tempdir = tempfile.TemporaryDirectory().name
        os.mkdir(tempdir)
        filename = f"{experiment_name}-{timestamp}-cv_results.csv"
        csv = os.path.join(tempdir, filename)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            pd.DataFrame(cv_results).to_csv(csv, index=False)

        mlflow.log_artifact(csv, f"{model_name}_cv_results")

        y_pred = model.predict(X_test)

        fig1 = plot_time_series(train_df, x_col = 'event_timestamp', y_col = target_feature, y_label_name = target_feature_alias)
        fig5 = plot_residuals(y_test, y_pred)
        fig7 = plot_prediction_error(y_test, y_pred)
        fig8 = plot_qq(y_test, y_pred)

        mlflow.log_figure(fig1, f"{model_name}_time_series_price.png")
        mlflow.log_figure(fig5, f"{model_name}_residuals_plot.png")
        mlflow.log_figure(fig7, f"{model_name}_prediction_errors.png")
        mlflow.log_figure(fig8, f"{model_name}_qq_plot.png")


        mlflow.log_artifact(corr_path)

        uri = f"runs:/{run.info.run_id}/{model_name}"

    mlflow.end_run()
    return uri


