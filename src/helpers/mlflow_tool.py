import numpy as np
from sklearn.preprocessing import OneHotEncoder
import mlflow

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

