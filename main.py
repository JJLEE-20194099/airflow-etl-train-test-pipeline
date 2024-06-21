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

from schema.realestate import RealEstateData


app = FastAPI()

@app.get("/healthcheck")
def healthcheck():
    """Function checking price prediction module"""
    return {
        "data": "200"
    }

@app.post("/predict-realestate")
def predict_realestate(body:RealEstateData):
    return body