from schema.realestate import ModelVersionEnum
import json
from tqdm import tqdm
from joblib import load
from utils.config import order_config
import numpy as np
import lightgbm as lgb

feature_version_config = {
    "v0": 0,
    "v1": 1,
    "v2": 2,
    "v3": 3,
    "v4": 4,
    "v5": 5,
}

def get_inference_by_city_version(city = 0, version:ModelVersionEnum = 'v3', df=None):
    prefix = 'hcm' if city == 0 else 'hn'

    path = f"/mnt/long/long/datn-feast/data/featureset/{prefix}_v{feature_version_config[version]}.json"

    featureset = json.load(open(path, 'r'))

    cat_cols = featureset['cat_cols']
    num_cols = featureset['num_cols']

    infer_val_dict = {}

    for model_name in tqdm(['abr', 'cat', 'etr', 'gbr', 'knr', 'la', 'lgbm', 'linear', 'mlp', 'rf', 'ridge', 'xgb']):
        model = load(f'/home/long/airflow/dags/models/{prefix}/{model_name}/{version}/model.joblib')
        if model_name == 'lgbm':
            model.booster_.save_model('lgbm_cache.txt')
            model = lgb.Booster(model_file='lgbm_cache.txt')


        order = order_config[model_name]

        df[cat_cols] = df[cat_cols].astype(np.int32)

        if order == "num-cat":
            all_cols = num_cols + cat_cols
        else:
            all_cols =  cat_cols + num_cols

        X = df[all_cols]
        infer_val_dict[model_name] = model.predict(X)[0].item()

    return infer_val_dict



