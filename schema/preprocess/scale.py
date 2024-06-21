import pandas as pd

mean_land_size_df_dict = {
    0: pd.read_csv('schema/preprocess/data/table/hcm_mean_land_size_df.csv'),
    1: pd.read_csv('schema/preprocess/data/table/hn_mean_land_size_df.csv')
}

def scale_data(obj):
    obj['is_street_house'] = 1 if obj['accessibility'] == 2 else 0
    obj['landSize_ratio'] = obj['landSize'] / obj['acreage']

    mean_land_size_df = mean_land_size_df_dict[obj['city']]
    obj['meanLandSize'] = mean_land_size_df[mean_land_size_df['administrative_genre'] == obj['administrative_genre']].iloc[0]['meanLandSize']

    obj['landSize_ratio_with_administrative_genre'] = obj['landSize'] / obj['meanLandSize']
    obj['acreage_ratio_with_meanLandSize'] = obj['acreage'] / obj['meanLandSize']

    del obj['meanLandSize']

    return obj





