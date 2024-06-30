from schema.realestate import ModelVersionEnum
import json
from tqdm import tqdm
from joblib import load
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

    infer_val_dict = dict()

    for model_name in tqdm(['abr', 'cat', 'etr', 'gbr', 'knr', 'la', 'lgbm', 'linear', 'mlp', 'rf', 'ridge', 'xgb']):
        model = load(f'/home/long/airflow/dags/models/{prefix}/{model_name}/{version}/model.joblib')
        if model_name == 'lgbm':
            model.booster_.save_model('lgbm_cache.txt')
            model = lgb.Booster(model_file='lgbm_cache.txt')


        order = order_config[model_name]

        if order == "num-cat":
            all_cols = num_cols + cal_cols
        else:
            all_cols =  cal_cols + num_cols

        X = df[all_cols]
        infer_val_dict[model_name] = model.predict(X)

    return infer_val_dict



