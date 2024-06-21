from joblib import dump, load
import numpy as np

gmm_config = {
    'num_of_restaurant_in_2000m_radius': 2,
    'num_of_restaurant_in_1000m_radius': 2,
    'num_of_bank_in_2000m_radius': 3,
    'num_of_marketplace_in_2000m_radius': 2,
    'num_of_cafe_in_1000m_radius': 2
}

def get_gmm_feature(body):
    prefix = 'hcm' if body['city'] == 0 else 'hn'

    gmm_cols = gmm_config.keys()

    obj = dict()

    for col in gmm_cols:
        gmm = load(f'schema/preprocess/data/gmm/{prefix}_gmm_{col}.joblib')

        data = np.array([[body[col]]])
        obj[f'gmm_{gmm_config[col]}_component_{col}'] = list(gmm.predict(data))[0].item()

    return obj