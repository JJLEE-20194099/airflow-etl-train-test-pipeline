from enum import Enum
from pydantic import BaseModel, model_validator
from typing import Union, List, Optional
import json

class ModelVersionEnum(int, Enum):
    v3 = 3
    v2 = 2
    v1 = 1
    v0 = 0
    v4 = 4
    v5 = 5

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
    modelname: MLEnum
    feature_set_version: ModelVersionEnum
    city: CityEnum

    class Config:
        use_enum_values = True