# import pandas as pd
# from schema.preprocess.quadtree import get_nearest_feature


# print(get_nearest_feature(21.0480535,105.8096705))

import json
with open('streets.json', encoding='utf-8') as f:
   streets = json.load(f)

def get_latlon_by_address(district, ward, street, city):
    city_streets = [item for item in streets if item['CITY'].lower() == city.lower()]
    district_streets = [item for item in city_streets if item['DISTRICT'].lower() == district.lower()]
    ward_streets = [item for item in district_streets if item['WARD'].lower() == ward.lower()]
    street_streets = [item for item in ward_streets if item['STREET'].lower() == street.lower()]

    return {
        "lat": street_streets[0]["LAT"],
        "lon": street_streets[0]["LNG"]
    }

# print(get_latlon_by_address("tây hồ", "Thụy Khuê", "Thụy Khuê",  "hà nội"))