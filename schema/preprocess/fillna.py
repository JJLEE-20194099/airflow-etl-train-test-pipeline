cat_cols = ['nearest_3_street',
 'nearest_2_street',
 'nearest_4_ward',
 'nearest_5_ward',
 'certificateOfLandUseRight',
 'nearest_7_street',
 'typeOfRealEstate',
 'nearest_4_street',
 'nearest_0_ward',
 'nearest_1_street',
 'district',
 'nearest_3_district',
 'nearest_8_district',
 'houseDirection',
 'nearest_5_street',
 'nearest_5_district',
 'nearest_4_district',
 'ward',
 'nearest_1_district',
 'nearest_7_district',
 'street',
 'nearest_0_district',
 'nearest_8_ward',
 'is_street_house',
 'nearest_3_ward',
 'nearest_0_street',
 'nearest_6_district',
 'nearest_6_ward',
 'nearest_8_street',
 'nearest_2_district',
 'nearest_6_street',
 'nearest_2_ward',
 'accessibility',
 'nearest_7_ward',
 'nearest_1_ward']

def fillna_cat(obj):
    for col in cat_cols:
        if obj[col] is None:
            obj[col] = 100

    return obj


