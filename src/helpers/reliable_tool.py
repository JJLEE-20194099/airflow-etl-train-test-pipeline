from feast import FeatureStore

from dotenv import load_dotenv
load_dotenv(override=True)
import os
import json

sample_dict = {
    "hcm": 35000,
    "hn": 100000
}


def get_trustworthy_dataset(city, version):

    FS = json.load(open(f'/home/long/long/datn-feast/feature_repo/src/config/featureset/{city}_v{version}.json'))
    feature_dict = json.load(open(FS['featureset_path'], 'r'))
    cat_cols = feature_dict['cat_cols']
    num_cols = feature_dict['num_cols']

    all_cols = cat_cols + num_cols
    feast_dataset_name = FS["feast_dataset_name"]


    fs = FeatureStore(repo_path=os.getenv('FEAST_FEATURE_REPO'))
    train_data_source_name = feast_dataset_name
    full_df = fs.get_saved_dataset(name=train_data_source_name).to_df()

    reliable_df = full_df.iloc[sample_dict[city]:]

    return reliable_df

# data = get_trustworthy_dataset("hcm", 3)
# print(data.shape)
# print(data.head(10))