from schema.realestate import ModelVersionEnum
import json

feature_version_config = {
    "full_features": "v1",
    "full_features_gmm": "v2",
    "full_features_gmm_pca": "v3"
}

def get_inference_cols_by_name(city = 0, version:ModelVersionEnum = 'full_features'):
    prefix = 'hcm' if city == 0 else 'hn'

    path = f"src/models/featureset/{prefix}/{feature_version_config[version]}.json"

    featureset = json.load(open(path, 'r'))

    cat_cols = featureset['cat_cols']
    num_cols = featureset['num_cols']
    all_cols = cat_cols + num_cols

    return all_cols



