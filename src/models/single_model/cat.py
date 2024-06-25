from catboost import CatBoostRegressor
from sklearn.model_selection import GridSearchCV
import random
from joblib import load

from src.models.single_model.own import BKPriceEstimator

def create_model(cat_idxs = None, pretrained_file = None):

    if cat_idxs:
        update_model = CatBoostRegressor(cat_features=cat_idxs, iterations=100, verbose = 100, random_state=random.randint(3, 1000))
    else:
        update_model = CatBoostRegressor(iterations=100, verbose = 100, random_state=random.randint(3, 1000))

    model = BKPriceEstimator(
        pretrained_model_path = pretrained_file,
        update_model = update_model,
    )

    search_params = {'weight': [0.01, 0.08, 0.15]}
    clf = GridSearchCV(model, search_params, scoring=['explained_variance', 'max_error', 'neg_root_mean_squared_error', 'r2', 'neg_median_absolute_error', 'neg_mean_absolute_percentage_error'], refit='neg_root_mean_squared_error', cv=5)
    return clf