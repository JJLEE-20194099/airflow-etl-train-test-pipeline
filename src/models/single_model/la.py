from sklearn.linear_model import Lasso
from sklearn.model_selection import GridSearchCV
import random
from joblib import load
def create_model(pretrained_file = None):
    if pretrained_file is None:
        model = Lasso(random_state=random.randint(3, 1000))
    else:
        model = load(pretrained_file)
    search_params = {'max_iter': [10 ,30]}
    clf = GridSearchCV(model, search_params, scoring=['explained_variance', 'max_error', 'neg_root_mean_squared_error', 'r2', 'neg_root_mean_squared_log_error', 'neg_median_absolute_error', 'neg_mean_absolute_percentage_error'], refit='neg_root_mean_squared_error', cv=5)
    return clf