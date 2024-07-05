

import pandas as pd
import numpy as np
from schema.preprocess.distance_tool import get_distance_feature
from schema.preprocess.facility import count_facility_inference
from schema.preprocess.fillna import fillna, nan_2_none
from schema.preprocess.gmm_tool import get_gmm_feature
from schema.preprocess.pca_tool import get_pca_feature
from schema.preprocess.quadtree import get_nearest_feature
from schema.preprocess.text import preprocess_text
from schema.preprocess.transform import get_compact_realestate_info

from schema.preprocess.encode import encoder_dict
from schema.preprocess.scale import mean_land_size_df_dict, rename_obj

from tqdm import tqdm
import json
import os
from datetime import datetime

city_dict = {"hcm": 0, "hn": 1}

def create_new_candidate_for_feast_fv(candidate_df, version_tag):
    path = f'/mnt/long/long/datn-feast/data/update_data/{version_tag}'
    new_config_path = f'/mnt/long/long/datn-feast/feature_repo/src/config/featureset/update_data/{version_tag}'
    os.makedirs(path, exist_ok=True)
    os.makedirs(new_config_path, exist_ok=True)

    fv_config_path_list = []
    for city in ['hn', 'hcm']:
        for version in tqdm(range(6)):

            df = candidate_df[candidate_df['city'] == city_dict[city]].reset_index(drop = True)

            feature_dict = json.load(open(f'/mnt/long/long/datn-feast/data/featureset/{city}_v{version}.json'))
            cat_cols = feature_dict['cat_cols']
            num_cols = feature_dict['num_cols']
            all_cols = list(set(cat_cols + num_cols))

            selected_cols = [c for c in df.columns.tolist() if c not in ['target', 'time']]

            df[cat_cols] = df[cat_cols].astype(np.int32)


            timestamps = df['time'].tolist()
            df['time'] = [pd.Timestamp(item) for item in timestamps]
            df = df.rename(columns = {'time': 'event_timestamp'})

            df = df.sort_values(by = ['event_timestamp'])
            df = df.reset_index(drop = True)


            full_feat_set = selected_cols

            data_df = df[all_cols + ['event_timestamp']]
            target_df = df[['target'] + ['event_timestamp']]

            realestate_ids = pd.DataFrame(data=list(range(len(data_df))), columns=["realestate_id"])

            data_df = pd.concat(objs=[data_df, realestate_ids], axis=1)
            target_df = pd.concat(objs=[target_df, realestate_ids], axis=1)

            featureset_df_path = f'{path}/data_df_{city}_v{version}.parquet'
            data_df.to_parquet(path=featureset_df_path)

            featureset_target_path = f'{path}/target_df_{city}_v{version}.parquet'
            target_df.to_parquet(path=featureset_target_path)

            config = {
                "df_feature_view_name": f"{version_tag}_df_{city}_feature_view_v{version}",
                "target_feature_view_name": f"{version_tag}_target_{city}_feature_view_v{version}",
                "featureset_path": f"/mnt/long/long/datn-feast/data/featureset/{city}_v{version}.json",
                "featureset_df_path": featureset_df_path,
                "featureset_target_path": featureset_target_path,
                "feast_dataset_name": f"{version_tag}_realestate_dataset_{city}_v{version}",
                "feast_dataset_path": f"{path}/realestate_dataset_{city}_v{version}.parquet"
            }

            fv_config_path = f"{new_config_path}/{city}_v{version}.json"
            with open(fv_config_path, "w") as outfile:
                json.dump(config, outfile)

            fv_config_path_list.append(fv_config_path)

    return fv_config_path_list


population_df = pd.read_csv('schema/preprocess/data/table/process_population.csv')

def build_offline_batch_data(standard_data):


    data = [get_compact_realestate_info(item) for item in standard_data]
    data = [item for item in data if item != {}]
    data = [item for item in data if item['street'] != None]
    data = [item for item in data if item['district'] != None]
    data = [item for item in data if item['lat'] != None and item['lon'] != None]
    data = [item for item in data if item['landSize'] != None and item['price'] != None]
    data = pd.DataFrame(data)

    data = data.drop_duplicates(subset = ['district', 'city', 'street', 'ward', 'numberOfFloors', 'numberOfLivingRooms', 'numberOfBathRooms', 'landType', 'price', 'description'], keep = 'first')
    data = data.reset_index(drop = True)

    print("Build Offline Batch Size:", len(data))
    print("Convert Obj Data to DataFrame")

    data['district'] = data['district'].apply(preprocess_text)
    data['city'] = data['city'].apply(preprocess_text)
    data = data[data['city'].isin(['hà nội', 'hồ chí minh'])]

    data['district'] = data['district'].replace('mê linh', 'suburb_west')
    data['district'] = data['district'].replace('ba vì', 'suburb_west')
    data['district'] = data['district'].replace('phúc thọ', 'suburb_west')
    data['district'] = data['district'].replace('thạch thất', 'suburb_west')
    data['district'] = data['district'].replace('mỹ đức', 'suburb_west')
    data['district'] = data['district'].replace('sơn tây', 'suburb_west')
    data['district'] = data['district'].replace('quốc oai', 'suburb_west')
    data['district'] = data['district'].replace('quốc oai', 'suburb_west')

    data['district'] = data['district'].replace('sóc sơn', 'suburb_north')
    data['district'] = data['district'].replace('đan phượng', 'suburb_north')

    data['district'] = data['district'].replace('thanh oai', 'suburb_south')
    data['district'] = data['district'].replace('ứng hòa', 'suburb_south')
    data['district'] = data['district'].replace('phú xuyên', 'suburb_south')
    data['district'] = data['district'].replace('thường tín', 'suburb_south')
    data['district'] = data['district'].replace('chương mỹ', 'suburb_south')

    data['ward'] = data['ward'].apply(preprocess_text)
    data['street'] = data['street'].apply(preprocess_text)
    data['description'] = data['description'].apply(preprocess_text)

    print("Handle Text in Data Done")

    data['endWidth'] = data['endWidth'].replace(0, np.nan)
    data['frontRoadWidth'] = data['frontRoadWidth'].replace(0, np.nan)
    data['numberOfFloors'] = data['numberOfFloors'].replace(0, np.nan)

    data['numberOfBathRooms'] = data['numberOfBathRooms'].replace(0, np.nan)
    data['numberOfLivingRooms'] = data['numberOfLivingRooms'].replace(0, np.nan)
    data['numberOfKitchens'] = data['numberOfKitchens'].replace(0, np.nan)
    data['numberOfGarages'] = data['numberOfGarages'].replace(0, np.nan)
    data['certificateOfLandUseRight'] == (data['certificateOfLandUseRight'] == 'yes').astype(np.int32)

    data = data[data['numberOfFloors'] <= 100].reset_index(drop = True)

    del data['frontRoadWidth']
    del data['distanceToNearestRoad']

    del data['unitPrice']
    del data['landType']

    data['w'] = data['frontWidth']
    data['h'] = data['landSize'] / data['w']

    data = data.replace(np.inf, np.nan)

    data = data.drop_duplicates().sample(frac = 1.0)

    hn_data = data[data['city'] == 'hà nội'].head(1000)
    hcm_data = data[data['city'] == 'hồ chí minh'].head(1000)

    data = pd.concat([hn_data, hcm_data])
    data = data.reset_index(drop = True)

    print("Start: make facility features")

    lat_list = data['lat'].tolist()
    lon_list = data['lon'].tolist()

    facility_list = []
    for lat, lon in tqdm(zip(lat_list, lon_list)):
        facility_count = count_facility_inference(lat, lon)
        facility_list.append(facility_count)

    facility_df = pd.DataFrame(facility_list)

    data = pd.concat([data, facility_df], axis = 1)

    print("End: make facility features")


    for col in [
            "city",
            "street",
            "ward",
            "district",
            "certificateOfLandUseRight",
            "typeOfRealEstate",
            "facade",
            "houseDirection",
            "accessibility"
        ]:
        data[col] = data[col].map(encoder_dict[col])

    data = data.merge(population_df, how='left', on = 'district')

    del data['numberOfKitchens']
    del data['numberOfGarages']

    print("Start: make distance features")

    city_list = data['city'].tolist()
    district_lat_list = data['district_lat'].tolist()
    district_lon_list = data['district_lon'].tolist()


    distance_list = []

    lat_list = data['lat'].tolist()
    lon_list = data['lon'].tolist()

    for i, lat in tqdm(enumerate(lat_list)):

        distance_list.append(get_distance_feature(city_list[i], lat, lon_list[i], district_lat_list[i], district_lon_list[i]))

    distance_df = pd.DataFrame(distance_list)

    data = pd.concat([data, distance_df], axis = 1)

    print("End: make distance features")

    data['target'] = data['price'] * 1000 / data['landSize']

    data['population'] = np.log(data['population'])
    data['density'] = np.log(data['density'])
    data['acreage'] = np.log(data['acreage'])

    print("Start: make nearest features")

    nearest_list = []
    for lat, lon in tqdm(zip(lat_list, lon_list)):
        nearest_dict = get_nearest_feature(lat, lon)
        nearest_list.append(nearest_dict)

    nearest_df = pd.DataFrame(nearest_list)

    data = pd.concat([data, nearest_df], axis = 1)

    print("End: make nearest features")

    data['is_street_house'] = (data['accessibility'] == 0).astype(np.int32)
    data['landSize_ratio'] = data['landSize'] / data['acreage']

    mean_land_size_list = []

    administrative_genre_list = data['administrative_genre'].tolist()

    for administrative_genre, city in zip(administrative_genre_list, city_list):
        mean_land_size_df = mean_land_size_df_dict[city]
        mean_land_size_list.append(mean_land_size_df[mean_land_size_df['administrative_genre'] == administrative_genre].iloc[0]['meanLandSize'])

    data['meanLandSize'] = mean_land_size_list

    data['landSize_ratio_with_administrative_genre'] = data['landSize'] / data['meanLandSize']
    data['acreage_ratio_with_meanLandSize'] = data['acreage'] / data['meanLandSize']

    del data['meanLandSize']

    obj_list = data.to_dict('records')
    obj_list = [rename_obj(obj) for obj in tqdm(obj_list)]
    obj_list = [nan_2_none(obj) for obj in tqdm(obj_list)]
    obj_list = [fillna(obj) for obj in tqdm(obj_list)]


    data = pd.DataFrame(obj_list)
    gmm_df = pd.DataFrame([get_gmm_feature(obj) for obj in obj_list])
    data = pd.concat([data, gmm_df], axis = 1)


    obj_list = data.to_dict('records')
    pca_df = pd.DataFrame([get_pca_feature(obj) for obj in obj_list])
    data = pd.concat([data, pca_df], axis = 1)

    version_tag = f'demo1'
    fv_config_path_list = create_new_candidate_for_feast_fv(data, version_tag)

    return {
        "fv_config_path_list": fv_config_path_list,
        "sample_data": nan_2_none(data.to_dict('records')[0]),
        "id_list": data["_id"].tolist(),
        "version_tag": version_tag
    }








