import json

cat_cols = ['nearest_3_street',
 'nearest_2_street',
 'nearest_4_ward',
 'nearest_5_ward',
 'certificateOfLandUseRight',
 'nearest_7_street',
 'typeOfRealEstate',
 'nearest_4_street',
 'nearest_0_ward',
 'nearest_1_street',
 'district',
 'nearest_3_district',
 'nearest_8_district',
 'houseDirection',
 'nearest_5_street',
 'nearest_5_district',
 'nearest_4_district',
 'ward',
 'nearest_1_district',
 'nearest_7_district',
 'street',
 'nearest_0_district',
 'nearest_8_ward',
 'is_street_house',
 'nearest_3_ward',
 'nearest_0_street',
 'nearest_6_district',
 'nearest_6_ward',
 'nearest_8_street',
 'nearest_2_district',
 'nearest_6_street',
 'nearest_2_ward',
 'accessibility',
 'nearest_7_ward',
 'nearest_1_ward']

def fillna(obj, cat_cols = cat_cols):
    for col in cat_cols:
        if obj[col] is None:
            obj[col] = 100

    city = obj['city']

    if city == 0:
        mean_dict = json.load(open('schema/preprocess/data/json/hcm_mean_num_col.json', 'r'))
        num_cols =  json.load(open('/mnt/long/long/datn-feast/data/featureset/hcm_v1.json', 'r'))['num_cols']
    else:
        mean_dict = json.load(open('schema/preprocess/data/json/hn_mean_num_col.json', 'r'))
        num_cols = json.load(open('/mnt/long/long/datn-feast/data/featureset/hn_v1.json', 'r'))['num_cols']
    for col in num_cols:
        if obj[col]:
            continue
        obj[col] = mean_dict[col]

    return obj

def nan_2_none(obj):
    if isinstance(obj, dict):
        return {k:nan_2_none(v) for k,v in obj.items()}
    elif isinstance(obj, list):
        return [nan_2_none(v) for v in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    return obj


