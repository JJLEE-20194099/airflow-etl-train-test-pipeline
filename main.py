import os
import json
from fastapi import FastAPI
from fastapi import HTTPException, File, UploadFile
from dataclasses import astuple
from fastapi.responses import FileResponse
import requests
from tqdm import tqdm
from pydantic import BaseModel
import csv
import pandas as pd
from dataclasses import dataclass
import numpy as np
import lightgbm as lgb
import math
from typing import Union, List, Optional
import regex as re
from datetime import datetime
from enum import Enum

from schema.preprocess.distance_tool import get_distance_feature
from schema.preprocess.facility import count_facility_inference, get_population_feature, log_val
from schema.preprocess.fillna import fillna_cat
from schema.preprocess.quadtree import get_nearest_feature
from schema.preprocess.scale import scale_data
from schema.realestate import RealEstateData
from schema.preprocess.encode import encoder_dict
import time


app = FastAPI()

@app.get("/healthcheck")
def healthcheck():
    """Function checking price prediction module"""
    return {
        "data": "200"
    }

@app.post("/predict-realestate")
def predict_realestate(body:RealEstateData):

    body = dict(body)

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

    body = fillna_cat(body)

    return body