import os
from fastapi import FastAPI
from fastapi import BackgroundTasks
import requests
import pymongo
import pandas as pd
from datetime import datetime

from src.helpers.build_offline_data_tool import build_offline_batch_data
from schema.preprocess.distance_tool import get_distance_feature
from schema.preprocess.facility import count_facility_inference, get_population_feature, log_val
from schema.preprocess.fillna import fillna
from schema.preprocess.gmm_tool import get_gmm_feature
from schema.preprocess.pca_tool import get_pca_feature
from schema.preprocess.scale import scale_data
from schema.preprocess.quadtree import get_nearest_feature
from schema.realestate import RealEstateData
from schema.version import ModelNameCityVersion
from schema.preprocess.encode import encoder_dict
from schema.mlops import MLOpsEXPData

from utils.infer_tool import get_inference_by_city_version
from utils.train_func import train_model_by_city_data_and_feature_version

from src.extensions.batching.batch_processor import BatchProcessor
from src.extensions.batching.models import BatchIn, BatchOut

from datetime import datetime, timedelta
import time

app = FastAPI()

@app.get("/healthcheck")
def healthcheck():
    """Function checking price prediction module"""
    return {
        "data": "200"
    }

@app.post("/start-clean-consumer")
def start_clean_consumer():
    os.system('python clean_raw_data.py')

@app.post("/build-training-dataset")
def build_training_dataset():
    feast_server = os.getenv('FEAST_SERVER')

    url = f"{feast_server}/build-training-dataset"

    payload = {}
    headers = {}

    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.json()


@app.post("/build-offline-batch-data")
def build_offline_batch(body: MLOpsEXPData):

    body = dict(body)
    exp_id = body['exp_id']

    connection_str = os.getenv('REALESTATE_DB')
    __client = pymongo.MongoClient(connection_str)

    database = 'realestate'
    __database = __client[database]

    collection = __database["realestate_listing"]

    # hn_offline_batch_data = list(collection.find({"propertyBasicInfo.address.value.city": "Hà Nội"}))
    # hcm_offline_batch_data = list(collection.find({"propertyBasicInfo.address.value.city": "Hồ Chí Minh"}))

    start_date = time.time() - 24 * 60 * 60 * 7

    full_offline_batch_data = list(collection.find({ "crawlInfo.db_create_timestamp": { "$gt": start_date } }))

    # result = build_offline_batch_data({
    #     "hn": hn_offline_batch_data,
    #     "hcm": hcm_offline_batch_data,
    # })

    print(full_offline_batch_data[0])
    result = build_offline_batch_data(full_offline_batch_data)

    collection = __database["dataset_metadata"]

    dataset_metadata = {
        "create_timestamp": time.time(),
        "id_list": result["id_list"],
        "size": len(result["id_list"]),
        "version_tag": result["version_tag"]
    }


    insert_result = collection.insert_one(dataset_metadata)

    del dataset_metadata['_id']

    result = {
        "dataset_id": f'{insert_result.inserted_id}',
        "fv_config_path_list": result["fv_config_path_list"],
        "sample_data": result["sample_data"],
        "value": dataset_metadata,
        "exp_id": exp_id
    }

    return result


priority_map = {
    'cat': 9,
    'lgbm': 9,
    'xgb': 9,
    'abr': 9,
    'etr': 9,
    'gbr': 9,
    'knr': 9,
    'la': 9,
    'linear': 9,
    'mlp': 9,
    'rf': 9,
    'ridge': 9
}

@app.post("/train-ai-model")
def train_ai_model(body: ModelNameCityVersion, background_tasks: BackgroundTasks):
    body = dict(body)

    city = body['city']
    feature_set_version = body['feature_set_version']
    modelname = body['modelname']

    if modelname in ['cat', 'lgbm', 'xgb']:
        train_model_by_city_data_and_feature_version.apply_async(
            (city, feature_set_version, modelname),
            queue=f'{city}_{modelname}',
            priority=priority_map[modelname]
        )
    else:
        train_model_by_city_data_and_feature_version.apply_async(
            (city, feature_set_version, modelname),
            queue=f'{city}_other',
            priority=priority_map[modelname]
        )
        # background_tasks.add_task(train_model_by_city_data_and_feature_version, city, feature_set_version, modelname)

    return 1

import json
with open('streets.json', encoding='utf-8') as f:
   streets = json.load(f)

def get_latlon_by_address(district, ward, street, city):
    city_streets = [item for item in streets if item['CITY'].lower() == city.lower()]
    district_streets = [item for item in city_streets if item['DISTRICT'].lower() == district.lower()]
    ward_streets = [item for item in district_streets if item['WARD'].lower() == ward.lower()]
    street_streets = [item for item in ward_streets if item['STREET'].lower() == street.lower()]

    return {
        "lat": float(street_streets[0]["LAT"]),
        "lon": float(street_streets[0]["LNG"])
    }

def process(body):
    try:
        body = dict(body)
    except:pass

    body = RealEstateData.parse_obj(body)

    try:
        body = dict(body)
    except:pass

    # print(body)

    body['time'] = datetime.now()

    body['w'] = body['w'] if 'w' in body and body['w'] != -1 else body['frontWidth']
    body['h'] = body['h'] if 'h' in body and body['h'] != -1 else body['landSize'] / body['w']

    district, ward, street = [item.strip().lower() for item in body['street'].split(" - ")]

    body['latlon'] = get_latlon_by_address(district, ward, street, body['city'])

    facility_count_dict = count_facility_inference(body['latlon']['lat'], body['latlon']['lon'])
    body = {**body, **facility_count_dict}

    body['lat'] = body['latlon']['lat']
    body['lon'] = body['latlon']['lon']

    if body['frontRoadWidth'] > 0:

        if body['frontRoadWidth'] <= 2.5:
            body['accessibility'] =  'theBottleNeckPoint'
        elif body['frontRoadWidth'] > 2.5 and body['frontRoadWidth'] <= 3:
            body['accessibility'] =  'narrorRoad'
        elif body['frontRoadWidth'] > 3 and body['frontRoadWidth'] <= 4:
            body['accessibility'] =  'fitOneCarAndOneMotorbike'
        elif body['frontRoadWidth'] > 4 and body['frontRoadWidth'] <= 5:
            body['accessibility'] =  'parkCar'
        elif body['frontRoadWidth'] > 5 and body['frontRoadWidth'] <= 7:
            body['accessibility'] =  'fitTwoCars'
        elif body['frontRoadWidth'] > 7:
            body['accessibility'] =  'fitThreeCars'
    else:
        body['accessibility'] = 'notInTheAlley'

    # print(body)

    for col in [
        "city",
        "street",
        "ward",
        "district",
        "certificateOfLandUseRight",
        "typeOfRealEstate",
        "facade",
        "houseDirection",
        "accessibility",
        "prefixDistrict"
    ]:
        try:
            body[col] = encoder_dict[col][body[col]]
        except:
            print(body[col], col)

    population_dict = get_population_feature(body['district'])
    body = {**body, **population_dict}

    distance_dict = get_distance_feature(body['city'], body['lat'], body['lon'], body['district_lat'], body['district_lon'])
    body = {**body, **distance_dict}

    body = log_val(body)

    nearest_dict = get_nearest_feature(body['lat'], body['lon'])
    body = {**body, **nearest_dict}
    body = scale_data(body)
    body = fillna(body)
    gmm_dict = get_gmm_feature(body)
    body = {**body, **gmm_dict}

    print(body)


    pca_dict = get_pca_feature(body)
    body = {**body, **pca_dict}
    return body

@app.post("/predict-realestate")
def predict_realestate(body:RealEstateData):

    body = process(body)
    df = pd.DataFrame([body])

    infer_val = get_inference_by_city_version(city = body['city'], version = body['version'], df = df)

    return infer_val

@app.post(
    "/predict-realestate-batch",
    tags = ["predict-realestate-batch"],
    responses={403: {"description": "Operation forbidden"}}
)
async def predict_realestate_batch(batch: BatchIn):
    body_list = [process(body.body) for body in batch.requests]

    body = batch.requests[0].body

    df = pd.DataFrame(body_list)

    return get_inference_by_city_version(city = body['city'], version = body['version'], df = df)


