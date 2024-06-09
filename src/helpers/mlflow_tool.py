import mlflow.pyfunc
import numpy as np
from sklearn.preprocessing import OneHotEncoder

def load_model_or_uri(model_or_uri):
    if isinstance(model_or_uri, str):
        return mlflow.pyfunc.load_model(model_uri=model_or_uri)
    else:
        return model_or_uri



