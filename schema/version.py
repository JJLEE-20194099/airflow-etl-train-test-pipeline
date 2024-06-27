from enum import Enum
from pydantic import BaseModel, model_validator
from typing import Union, List, Optional
import json

class ModelVersionEnum(str, Enum):
    v3 = 'v3'
    v2 = 'v2'
    v1 = 'v1'
    v0 = 'v0'
    v4 = 'v4'
    v5 = 'v5'

class MLEnum(str, Enum):
    cat = 'cat'
    lgbm = 'lgbm'
    xgb = 'xgb'
    abr = 'abr'
    etr = 'etr'
    gbr = 'gbr'
    knr = 'knr'
    la = 'la'
    linear = 'linear'
    mlp = 'mlp'
    rf = 'rf'
    ridge = 'ridge'

class CityEnum(str, Enum):
    hn = 'hn'
    hcm = 'hcm'


class ModelNameCityVersion(BaseModel):
    model_name: MLEnum
    feature_set_version: ModelVersionEnum
    city: CityEnum