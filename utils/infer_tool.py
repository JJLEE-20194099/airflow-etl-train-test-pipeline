from schema.realestate import ModelVersionEnum
import json
from tqdm import tqdm
from joblib import load
from utils.config import order_config
import numpy as np
import lightgbm as lgb
import pandas as pd

feature_version_config = {
    "v0": 0,
    "v1": 1,
    "v2": 2,
    "v3": 3,
    "v4": 4,
    "v5": 5,
}

min_max_df = pd.read_csv('utils/minmax.csv')

def minmax_refine(val, district):
    t = min_max_df[min_max_df["district"] == district]
    min_val = t['target_min'].iloc[0]
    max_val = t['target_max'].iloc[0]

    if val < min_val:
        return min_val
    if val > max_val:
        return max_val
    return val

def bkpostprocessing(val, district):
    return minmax_refine(val, district)


def get_inference_by_city_version(city = 0, version:ModelVersionEnum = 'v3', df=None):
    prefix = 'hcm' if city == 0 else 'hn'

    path = f"/mnt/long/long/datn-feast/data/featureset/{prefix}_v{feature_version_config[version]}.json"

    featureset = json.load(open(path, 'r'))

    cat_cols = featureset['cat_cols']
    num_cols = featureset['num_cols']

    infer_val_dict = {}


    for model_name in tqdm(['cat','lgbm', 'xgb']):
    # for model_name in tqdm(['cat','lgbm', 'xgb', 'etr', 'rf']):
    # for model_name in tqdm(['abr', 'cat', 'etr', 'gbr', 'knr', 'la', 'lgbm', 'linear', 'mlp', 'rf', 'ridge', 'xgb']):

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
    bonus1 = 0
    bonus2 = 0
    if df['facility_check_ok'].tolist()[0]:
        bonus2 = 58.23

    if df['narrow_alley'].tolist()[0] == 1:
        bonus1+= 58.23

    if df['narrow_alley'].tolist()[0] == 2:
        bonus1 += 58.23 / 2

    if bonus1 > 0 or bonus2 > 0:
        infer_val_dict["bagging"] = max(infer_val_dict["cat"], infer_val_dict["lgbm"], infer_val_dict["xgb"]) + bonus2 / 10
    else:
        infer_val_dict["bagging"] = (infer_val_dict["cat"] * 10 + infer_val_dict["lgbm"] * 10 + infer_val_dict["xgb"] * 80 ) / 100

    val = infer_val_dict["bagging"] + bonus1 + bonus2
    district = df['district'].tolist()[0]
    infer_val_dict["BKPrice System PostProcessing"] = bkpostprocessing(val, district)

    return infer_val_dict



