from sklearn.linear_model import Lasso
from sklearn.model_selection import GridSearchCV
import random
from joblib import load

from src.models.single_model.own import BKPriceEstimator
def create_model(pretrained_file = None):

    update_model = Lasso(random_state=random.randint(3, 1000), max_iter = 100)
    model = BKPriceEstimator(
        pretrained_model_path = pretrained_file,
        update_model = update_model,
    )
    search_params = {'weight': [-0.001, 0, 0.001]}
    clf = GridSearchCV(model, search_params, scoring=['explained_variance', 'max_error', 'neg_root_mean_squared_error', 'r2', 'neg_median_absolute_error', 'neg_mean_absolute_percentage_error'], refit='neg_root_mean_squared_error', cv=5)
    return clf