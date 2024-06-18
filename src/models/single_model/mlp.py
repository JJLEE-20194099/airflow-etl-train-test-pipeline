from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import GridSearchCV
import random

def create_model():
    model = MLPRegressor(random_state=random.randint(3, 1000))
    search_params = {'max_iter': [1000, 3000]}
    clf = GridSearchCV(model, search_params, scoring=['explained_variance', 'max_error', 'neg_root_mean_squared_error', 'r2', 'neg_root_mean_squared_log_error', 'neg_median_absolute_error', 'neg_mean_absolute_percentage_error'], refit='neg_root_mean_squared_error', cv=5)
    return clf