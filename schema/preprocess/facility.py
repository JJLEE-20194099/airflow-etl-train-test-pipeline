import pandas as pd
import json
import numpy as np
from tqdm import tqdm
from dataclasses import astuple
import requests
from dataclasses import dataclass
import math

@dataclass
class LocationConfig:
    lat: float
    lon: float
    distance: int

class OpenstreetMap:
    MAIN_DOMAIN = 'http://65.109.112.52'
    API_PATH = '/api/interpreter'

class MeanOfFacility:
    TOWNHALL = 'townhall'
    COMMUNITY_CENTER = 'community_centre'

means_of_facility_obj = {
        "means_of_facility_list": [
            "university",
            "fuel",
            "cafe",
            "parking",
            "parking_entrance",
            "fast_food",
            "marketplace",
            "restaurant",
            "hospital",
            "school",
            "kindergarten",
            "townhall - community_centre",
            "police",
            "place_of_worship",
            "bank",
            "atm"
        ]
    }
means_of_facility = means_of_facility_obj["means_of_facility_list"]


calculate_facility_dict = {
    500:  pd.read_csv('schema/preprocess/data/table/hcm_hn_distance_500_facility_count.csv'),
    1000:  pd.read_csv('schema/preprocess/data/table/hcm_hn_distance_1000_facility_count.csv'),
    2000:  pd.read_csv('schema/preprocess/data/table/hcm_hn_distance_2000_facility_count.csv')
}


def findpublicfacilities(location_config: LocationConfig):

    lat, lon, distance = astuple(location_config)
    overpass_url = f"{OpenstreetMap.MAIN_DOMAIN}{OpenstreetMap.API_PATH}"

    overpass_query = f"""
      [out:json];
      (
      node["amenity"=""](around:{distance},{lat},{lon});
      way["amenity"=""](around:{distance},{lat},{lon});
      rel["amenity"=""](around:{distance},{lat},{lon});
      );
      out center;
      """

    response = requests.get(overpass_url,
                            params={'data': overpass_query}, timeout=60)

    data = response.json()

    return data['elements']


def count_facilities(lat: float, lon: float, distance: int, response = None):
    if response is not None:
        res = response
    else:
        try:
            res = findpublicfacilities(LocationConfig(
                lat = lat,
                lon = lon,
                distance = distance
            ))
        except Exception as e:
            print(e)
            return {}
    facility_dict = {}
    for _fa in means_of_facility:
        facility_dict[_fa] = 0

    for place in res:
        try:
            if place['type'] == MeanOfFacility.TOWNHALL or place['type'] == MeanOfFacility.COMMUNITY_CENTER:
                facility_dict[f'{MeanOfFacility.TOWNHALL - MeanOfFacility.COMMUNITY_CENTER}'] = facility_dict[f'{MeanOfFacility.TOWNHALL - MeanOfFacility.COMMUNITY_CENTER}'] + 1
            if place['tags']['amenity'] in means_of_facility:
                facility_dict[place['tags']['amenity']
                            ] = facility_dict[place['tags']['amenity']] + 1

        except:
            pass

    return facility_dict


def rename_facility_col(obj, cols, distance):
    rename_cols = [f'num_of_{col}_in_{distance}m_radius' for col in cols]

    rename_col_dict = dict(zip(cols, rename_cols))

    result = dict()

    for col in cols:
        result[rename_col_dict[col]] = obj[col]

    return result

def count_facility_inference(lat, lon):

    result = dict()

    for distance in [500, 1000, 2000]:

        calculate_facility = calculate_facility_dict[distance]
        check_df = calculate_facility[(calculate_facility['lat'] == lat) & (calculate_facility['lon'] == lon)]

        if check_df.shape[0]:
            num_of_facility = check_df.iloc[0].to_dict()
            del num_of_facility['lat'], num_of_facility['lon']
        else:
            print("Cache Miss - facility Counter:", lat, lon)
            num_of_facility_dict = {}
            num_of_facility = count_facilities(lat, lon, distance=distance)

        obj = rename_facility_col(num_of_facility, means_of_facility, distance)

        result = {**result, **obj}

    return result

population_df = pd.read_csv('schema/preprocess/data/table/process_population.csv')

def get_population_feature(district):
    obj = population_df[population_df['district'] == district].iloc[0].to_dict()
    del obj['district']

    return obj

def log_val(obj):

    obj['population'] = math.log(obj['population'])
    obj['density'] = math.log(obj['density'])
    obj['acreage'] = math.log(obj['acreage'])

    return obj

