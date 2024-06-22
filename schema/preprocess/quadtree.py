import pandas as pd
import quads
import numpy as np
import math

import math
from tqdm import tqdm
def distance_func(lat1: float, lon1: float, lat2: float, lon2: float):

    try:
        R = 6371
        dLat = (lat2-lat1) * math.pi / 180
        dLon = (lon2-lon1) * math.pi / 180
        lat1 = lat1 * math.pi / 180
        lat2 = lat2 * math.pi / 180
        a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * \
            math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = R * c
        return d*1000
    except:
        print(f"Distance_Func error with: {(lat1, lon1, lat2, lon2)}")


lat_lon_df = pd.read_csv('schema/preprocess/data/table/realestate_lat_lon.csv')

tree = quads.QuadTree((15.86914545, 106.13803025), 12 , 2)

lat_list, lon_list = lat_lon_df['lat'].tolist(), lat_lon_df['lon'].tolist()
lat_lon_tuple_list = [(lat, lon) for lat, lon in zip(lat_list, lon_list)]

for lat_lon_tuple in lat_lon_tuple_list:
    tree.insert(lat_lon_tuple)

df = pd.read_csv('schema/preprocess/data/table/encode_location.csv')

def get_address_by_lat_lon(lat, lon):
    item_df = df[(df['lat'] == lat) & (df['lon'] == lon)]
    return item_df['district'].mode().tolist()[0], item_df['street'].mode().tolist()[0], item_df['ward'].mode().tolist()[0]

def find_nearest_neighbors(lat, lon, count = 10):
    found = tree.nearest_neighbors((lat, lon), count=count)
    nearest_lat_lon_df = pd.DataFrame()
    nearest_lat_lon_df['lat'] = [item.x for item in found]
    nearest_lat_lon_df['lon'] = [item.y for item in found]

    nearest_lat_lon_df['district'], nearest_lat_lon_df['street'], nearest_lat_lon_df['ward'] = zip(*nearest_lat_lon_df.apply(lambda x: get_address_by_lat_lon(x['lat'], x['lon']), axis = 1))

    arr = np.concatenate(nearest_lat_lon_df.values)
    return arr

def get_nearest_feature(lat, lon):
    arrs = [find_nearest_neighbors(lat, lon)]
    nearest_df = pd.DataFrame(arrs)

    nearest_df = pd.DataFrame(arrs)
    nearest_df = nearest_df.rename(columns = {0: 'lat', 1: 'lon'})
    del nearest_df[2],nearest_df[3], nearest_df[4]
    cols = [c for c in nearest_df.columns.tolist() if c not in ['lat', 'lon']]

    lat_cols = [c for c in nearest_df.columns.tolist() if c not in ['lat', 'lon'] and c % 5 == 0]
    format_lat_cols = [f'nearest_{i}_lat' for i, col in enumerate(lat_cols)]

    lon_cols = [c for c in nearest_df.columns.tolist() if c not in ['lat', 'lon'] and c % 5 == 1]
    format_lon_cols = [f'nearest_{i}_lon' for i, col in enumerate(lon_cols)]

    district_cols = [c for c in nearest_df.columns.tolist() if c not in ['lat', 'lon'] and c % 5 == 2]
    format_district_cols = [f'nearest_{i}_district' for i, col in enumerate(district_cols)]

    street_cols = [c for c in nearest_df.columns.tolist() if c not in ['lat', 'lon'] and c % 5 == 3]
    format_street_cols = [f'nearest_{i}_street' for i, col in enumerate(street_cols)]

    ward_cols = [c for c in nearest_df.columns.tolist() if c not in ['lat', 'lon'] and c % 5 == 4]
    format_ward_cols = [f'nearest_{i}_ward' for i, col in enumerate(ward_cols)]

    source_cols_list = [lat_cols, lon_cols, district_cols, street_cols, ward_cols]
    target_cols_list = [format_lat_cols, format_lon_cols, format_district_cols, format_street_cols, format_ward_cols]


    for source_cols, target_cols in zip(source_cols_list, target_cols_list):
        rename_dict = dict(zip(source_cols, target_cols))
        nearest_df = nearest_df.rename(columns = rename_dict)

    del nearest_df['lat']
    del nearest_df['lon']


    obj = nearest_df.iloc[0].to_dict()
    obj['lat'] = lat
    obj['lon'] = lon

    for i in range(9):
        try:
            obj[f'distance_nearest_{i}'] = distance_func(lat, lon, obj[f'nearest_{i}_lat'], obj[f'nearest_{i}_lon'])
            obj[f'distance_nearest_{i}'] = math.log(obj[f'distance_nearest_{i}'])
        except:
            obj[f'distance_nearest_{i}'] = np.nan

    return obj


