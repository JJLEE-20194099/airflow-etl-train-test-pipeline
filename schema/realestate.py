from enum import Enum
from pydantic import BaseModel, model_validator
from typing import Union, List, Optional
import json

from schema.preprocess.text import preprocess_text

class GeolocationModel(BaseModel):
    latitude: float
    longitude: float

class PrefixDistrictEnum(str, Enum):
    level_1 = 'quận'
    level_2 = 'huyện'

data = json.load(open('schema/expectations/address.json', 'r'))

class RealEstateData(BaseModel):
    landSize: float
    city: str
    district: str
    ward: str
    prefixDistrict: PrefixDistrictEnum
    street: str
    numberOfFloors: Optional[float] = 4
    numberOfBathRooms: Optional[float] = 3
    numberOfLivingRooms: Optional[float] = 3

    @model_validator(mode='before')
    def validate_compatibility_params_and_strategy_type(cls, field_values):

        city = field_values['city']
        district = field_values['district']
        ward = field_values['ward']
        street = field_values['street']
        landSize = field_values['landSize']
        street = field_values['street']
        prefixDistrict = field_values['prefixDistrict']

        if city is None:
            assert  "Missing City"


        if city not in ["hcm", "hn"]:
            assert city in ["hcm", "hn"], "Valid cities: hcm, hn"

        if city == "hn":

            assert  district in data[city]["district"], f"Valid districts: {data[city]['district']}"

            assert  ward in data[city]["ward"], f"Valid wards: {data[city]['ward']}"

            assert  street in data[city]["street"], f"Valid streets: {data[city]['street']}"

            for key in ['ward', 'district', 'street', 'prefixDistrict']:
                field_values[key] = preprocess_text(field_values[key])

            if district in ['mê linh',
                'ba vì',
                'phúc thọ',
                'thạch thất',
                'mỹ đức',
                'sơn tây',
                'quốc oai',
                'quốc oai']:

                field_values['district'] = 'suburb_west'

            if district in ['sóc sơn', 'đan phượng']:
                field_values['district'] = 'suburb_north'

            if district in ['thanh oai',
                'ứng hòa',
                'phú xuyên',
                'thường tín',
                'chương mỹ'
            ]:
                field_values['district'] = 'suburb_south'

        return field_values



