import mlflow.pyfunc
import numpy as np
import copy
from sklearn.preprocessing import OneHotEncoder

def load_model_or_uri(model_or_uri):
    if isinstance(model_or_uri, str):
        return mlflow.pyfunc.load_model(model_uri=model_or_uri)
    else:
        return model_or_uri

def stack_with_onehot(scores):
    new_scores_l = []
    for score in scores:
        n_score = score
        if len(score.shape) == 1:
            enc = OneHotEncoder()
            enc.fit([[i] for i in range(np.min(score), np.max(score) + 1)])
            n_score = enc.transform(np.expand_dims(score, -1)).toarray()
        new_scores_l.append(n_score)
    scores = np.array(new_scores_l)
    return scores