from catboost import CatBoostRegressor
from sklearn.model_selection import GridSearchCV
import random
from joblib import load

def create_model(cat_idxs = None, pretrained_file = None):
    if pretrained_file is None:
        if cat_idxs:
            model = CatBoostRegressor(cat_features=cat_idxs, iterations=10, verbose = 100, random_state=random.randint(3, 1000), task_type='GPU', devices='0:1')
        else:
            model = CatBoostRegressor(iterations=10, verbose = 100, random_state=random.randint(3, 1000), task_type='GPU', devices='0:1')
    else:
        model = load(pretrained_file)
    search_params = {'learning_rate': [0.01, 0.08, 0.15]}
    clf = GridSearchCV(model, search_params, scoring=['explained_variance', 'max_error', 'neg_root_mean_squared_error', 'r2', 'neg_root_mean_squared_log_error', 'neg_median_absolute_error', 'neg_mean_absolute_percentage_error'], refit='neg_root_mean_squared_error', cv=5)
    return clf