from schema.realestate import ModelVersionEnum
import json

feature_version_config = {
    "v0": 0,
    "v1": 1,
    "v2": 2,
    "v3": 3,
    "v4": 4,
}

def get_inference_cols_by_name(city = 0, version:ModelVersionEnum = 'v3'):
    prefix = 'hcm' if city == 0 else 'hn'

    path = f"/mnt/long/long/datn-feast/data/featureset/{prefix}_v{feature_version_config[version]}.json"

    featureset = json.load(open(path, 'r'))

    cat_cols = featureset['cat_cols']
    num_cols = featureset['num_cols']
    all_cols = cat_cols + num_cols

    return all_cols



