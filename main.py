import os
import json
from fastapi import FastAPI
from fastapi import APIRouter, Depends, BackgroundTasks
from fastapi import HTTPException, File, UploadFile
from dataclasses import astuple
from fastapi.responses import FileResponse
import requests
from tqdm import tqdm
from pydantic import BaseModel
import csv
import pymongo
import pandas as pd
from dataclasses import dataclass
import numpy as np
import lightgbm as lgb
import math
from typing import Union, List, Optional
import regex as re
from datetime import datetime
from enum import Enum

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
import time

from utils.infer_tool import get_inference_cols_by_name
from utils.train_func import train_model_by_city_data_and_feature_version


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
def build_offline_batch():

    connection_str = os.getenv('REALESTATE_DB')
    __client = pymongo.MongoClient(connection_str)

    database = 'realestate'
    __database = __client[database]

    collection = __database["realestate_listing"]

    # hn_offline_batch_data = list(collection.find({"propertyBasicInfo.address.value.city": "Hà Nội"}))
    # hcm_offline_batch_data = list(collection.find({"propertyBasicInfo.address.value.city": "Hồ Chí Minh"}))
    full_offline_batch_data = list(collection.find({}))

    # result = build_offline_batch_data({
    #     "hn": hn_offline_batch_data,
    #     "hcm": hcm_offline_batch_data,
    # })

    result = build_offline_batch_data(full_offline_batch_data)

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
        background_tasks.add_task(train_model_by_city_data_and_feature_version, city, feature_set_version, modelname)

    return 1

@app.post("/predict-realestate")
def predict_realestate(body:RealEstateData):

    body = dict(body)

    body['time'] = datetime.now()

    body['w'] = body['w'] if 'w' in body and body['w'] != -1 else body['frontWidth']
    body['h'] = body['h'] if 'h' in body and body['h'] != -1 else body['landSize'] / body['w']


    facility_count_dict = count_facility_inference(body['latlon'].lat, body['latlon'].lon)
    body = {**body, **facility_count_dict}

    body['lat'] = body['latlon'].lat
    body['lon'] = body['latlon'].lon


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


    pca_dict = get_pca_feature(body)
    body = {**body, **pca_dict}

    infer_cols = get_inference_cols_by_name(city = body['city'], version = body['version'])


    return infer_cols