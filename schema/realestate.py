from enum import Enum
from pydantic import BaseModel, model_validator
from typing import Union, List, Optional
import json

from schema.preprocess.text import concat, preprocess_text

class GeolocationModel(BaseModel):
    lat: float
    lon: float

class PrefixDistrictEnum(str, Enum):
    level_1 = 'quận'
    level_2 = 'huyện'

class TypeOfRealEstateEnum(str, Enum):
    condominium = "condominium"
    privateProperty = "privateProperty"
    privateLand = "privateLand"
    townhouse = "townhouse"
    semiDetachedVilla = "semiDetachedVilla"
    otherTypesOfProperty = "otherTypesOfProperty"
    shophouse = "shophouse"
    resort = "resort"

class FacadeEnum(str, Enum):
    oneSideOpen = "oneSideOpen"
    threeSideOpen = "threeSideOpen"
    twoSideOpen = "twoSideOpen"
    fourSideOpen = "fourSideOpen"

class HouseDirectionEnum(str, Enum):
    southeast = "southeast"
    southwest = "southwest="
    northeast = "northeast"
    northwest = "northwest"
    south = "south"
    east = "east"
    west = "west"
    north = "north"

class AccessibilityEnum(str, Enum):
    notInTheAlley = "notInTheAlley"
    fitThreeCars = "fitThreeCars"
    parkCar = "notInTheAlley"
    fitOneCarAndOneMotorbike = "fitOneCarAndOneMotorbike"
    fitTwoCars = "fitTwoCars"
    theBottleNeckPoint = "theBottleNeckPoint"
    narrorRoad = "narrorRoad"



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

    certificateOfLandUseRight: Optional[bool] = True

    typeOfRealEstate: TypeOfRealEstateEnum = "privateLand"
    facade: FacadeEnum = "oneSideOpen"
    houseDirection: HouseDirectionEnum = "east"
    accessibility: AccessibilityEnum = "fitThreeCars"

    endWidth: float
    frontRoadWidth: float
    frontWidth: float

    latlon: GeolocationModel

    @model_validator(mode='before')
    def validate_compatibility_params_and_strategy_type(cls, field_values):

        city = field_values['city']
        district = field_values['district']
        ward = field_values['ward']
        street = field_values['street']
        landSize = field_values['landSize']
        street = field_values['street']
        prefixDistrict = field_values['prefixDistrict']
        numberOfFloors = field_values['numberOfFloors']



        assert city, "Missing City"
        assert district, "Missing District"
        assert ward, "Missing Ward"
        assert street, "Missing Street"
        assert city in ["hcm", "hn"], "Valid cities: hcm, hn"
        assert numberOfFloors < 100, "Invalid value: numberOfFloors < 100"


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

            full_ward = concat(field_values['district'], field_values['ward'])
            full_street = concat(full_ward, field_values['street'])


            assert full_ward in data[city]["full_ward"], f'Ward {ward} not belong to District {district} - Valid: {data[city]["full_ward"]}'
            assert full_street in data[city]["full_street"], f'Street {street} not belong to Ward {ward} - District {district} - Valid: {data[city]["full_street"]}'

            field_values['ward'] = full_ward
            field_values['street'] = full_street

        else:
            assert  district in data[city]["district"], f"Valid districts: {data[city]['district']}"

            assert  ward in data[city]["ward"], f"Valid wards: {data[city]['ward']}"

            assert  street in data[city]["street"], f"Valid streets: {data[city]['street']}"

            for key in ['ward', 'district', 'street', 'prefixDistrict']:
                field_values[key] = preprocess_text(field_values[key])

            full_ward = concat(field_values['district'], field_values['ward'])
            full_street = concat(full_ward, field_values['street'])


            assert full_ward in data[city]["full_ward"], f'Ward {ward} not belong to District {district} - Valid: {data[city]["full_ward"]}'
            assert full_street in data[city]["full_street"], f'Street {street} not belong to Ward {ward} - District {district} - Valid: {data[city]["full_street"]}'

            field_values['ward'] = full_ward
            field_values['street'] = full_street

        return field_values



