import pandas as pd

from joblib import dump, load
import json

def get_pca_feature(obj):
    df = pd.DataFrame([obj])

    prefix = 'hcm' if obj['city'] == 0 else 'hn'
    if obj['city'] == 0:
        num_cols = json.load(open('/mnt/long/long/datn-feast/data/featureset/hcm_v2.json', 'r'))['num_cols']
    else:
        num_cols = json.load(open('/mnt/long/long/datn-feast/data/featureset/hn_v2.json', 'r'))['num_cols']

    pca = load(f'schema/preprocess/data/pca/{prefix}_pca.joblib')
    pca_data = pca.transform(df[num_cols])[0]

    obj['PC1'] = pca_data[0].item()
    obj['PC2'] = pca_data[1].item()

    return obj





