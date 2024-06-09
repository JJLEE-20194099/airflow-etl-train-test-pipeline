import numpy as np
from sklearn.preprocessing import OneHotEncoder
import mlflow
from mlflow.entities import ViewType
from mlflow.types.schema import Schema

from dotenv import load_dotenv
load_dotenv(override=True)
import os

def load_model_or_uri(model_or_uri):
    if isinstance(model_or_uri, str):
        return mlflow.pyfunc.load_model(model_uri=model_or_uri)
    else:
        return model_or_uri

def create_experiment(experiment_name):
    existing_exp = mlflow.get_experiment_by_name(experiment_name)
    if not existing_exp:
        experiment_id = mlflow.create_experiment(experiment_name)
        print("Create Experiment Successfully")
    else:
        experiment_id = existing_exp.experiment_id

    return experiment_id

def get_best_model(experiment_name, metric = 'best_cv_score'):
    mlflow.set_tracking_uri(os.getenv('MLFLOW_SERVER'))
    df = mlflow.search_runs(experiment_names=[experiment_name], run_view_type=ViewType.ACTIVE_ONLY)
    df = df.sort_values(by=[f'metrics.{metric}'], ascending=False)
    uri = None
    for _, row in df.iterrows():
        try:
            uri = "runs:/" + row['run_id'] + "/best_estimator"
            model = mlflow.sklearn.load_model(uri)
            if model is not None:
                print(f"loaded model = {row['run_id']}")
                break
        except:
            pass

    return model, uri

def get_feature_set_by_uri(uri):
    mlflow.set_tracking_uri(os.getenv('MLFLOW_SERVER'))

    schema = mlflow.pyfunc.load_model(uri).metadata.signature
    feature_names = Schema.input_names(schema.inputs)
    # target_name = Schema.input_names(schema.outputs)
    return feature_names
