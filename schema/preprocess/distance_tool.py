import pandas as pd
import math
city_dict = {"hồ chí minh": 0, "hà nội": 1}

best_df1 = pd.read_csv('schema/preprocess/data/table/hcm_famous_place.csv')
best_df1['city'] = 'hồ chí minh'

best_df2 = pd.read_csv('schema/preprocess/data/table/hn_famous_place.csv')
best_df2['city'] = 'hà nội'

best_df = pd.concat([best_df1, best_df2])

del best_df['address']
best_df['city'] = best_df['city'].map(city_dict)

import math
from tqdm import tqdm
def distance_func(lat1: float, lon1: float, lat2: float, lon2: float):

    try:
        R = 6371
        dLat = (lat2-lat1) * math.pi / 180
        dLon = (lon2-lon1) * math.pi / 180
        lat1 = lat1 * math.pi / 180
        lat2 = lat2 * math.pi / 180
        a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * \
            math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = R * c
        return d*1000
    except:
        print(f"Distance_Func error with: {(lat1, lon1, lat2, lon2)}")

def distance(lat1, lon1, lat2, lon2, city1, city2):
    if city1 != city2:
        return -1
    return distance_func(lat1, lon1, lat2, lon2)


best_arr = best_df.values

distance_cols = ['distance_hn_TTTM Saigon Centre (Takashimaya)',
 'distance_hn_Crescent Mall',
 'distance_hn_SC VivoCity Shopping Center',
 'distance_hn_Aeon Mall Shopping Center Tân Phú',
 'distance_hn_Saigon Garden',
 'distance_hn_Vincom Đồng Khởi',
 'distance_hn_Diamond Plaza Shopping Center',
 'distance_hn_Công viên Vinhomes Central Park',
 'distance_hn_Công viên khu đô thị Sala',
 'distance_hn_Công viên trên mây tại Taka Shimaya',
 'distance_hn_Công viên cá Koi Rin Rin Park',
 'distance_hn_Công viên cầu Ánh Sao – Hồ Bán Nguyệt',
 'distance_hn_Công viên Thỏ Trắng',
 'distance_hn_Thảo Cầm Viên',
 'distance_hn_Công viên Tao Đàn',
 'distance_hn_Công viên Lê Văn Tám',
 'distance_hn_Công viên 30-4',
 'distance_hn_Công viên nước Đầm Sen',
 'distance_hn_Công viên phép thuật Harry Potter',
 'distance_hn_Công viên Gia Định',
 'distance_hn_Công viên 23-9',
 'distance_hn_Công viên nước Củ Chi',
 'distance_hn_Công viên Hoàng Văn Thụ',
 'distance_hn_Đường Nguyễn Huệ, quận 1, TPHCM',
 'distance_hn_Đường Lê Lợi, quận 1, TPHCM',
 'distance_hn_Đường Đồng Khởi, quận 1, TPHCM',
 'distance_hn_Đường Lê Anh Xuân, quận 1',
 'distance_hn_Đường Lê Duẩn',
 'distance_hn_Đường Mạc Thị Bưởi',
 'distance_hn_Công trường Lam Sơn',
 'distance_hn_Đường Nam Kỳ Khởi Nghĩa',
 'distance_hn_Đường Mai Chí Thọ',
 'distance_hn_Đường Nguyễn Thị Định',
 'distance_hn_Đường Võ Chí Công',
 'distance_hn_Đường Hai Bà Trưng',
 'distance_hn_Nguyễn Thị Minh Khai',
 'distance_hn_Lê Văn Sỹ',
 'distance_hn_Bà Huyện Thanh Quan',
 'distance_hn_Đường Nguyễn Tất Thành',
 'distance_hn_Bến Vân Đồn',
 'distance_hn_Tôn Thất Thuyết',
 'distance_hn_Đoàn Văn Bơ',
 'distance_hn_Nguyễn Tri Phương',
 'distance_hn_Hùng Vương',
 'distance_hn_Lê Hồng Phong',
 'distance_hn_Trần Hưng Đạo',
 'distance_hn_Hậu Giang',
 'distance_hn_Nguyễn Văn Luông',
 'distance_hn_Kinh Dương Vương',
 'distance_hn_Nguyễn Văn Linh',
 'distance_hn_Đào Trí',
 'distance_hn_Nguyễn Thị Thập',
 'distance_hcm_Vincom Times City',
 'distance_hcm_Lotte Center Hanoi',
 'distance_hcm_Aeon Mall Hà Đông',
 'distance_hcm_Trung tâm thương mại Royal City Hà Nội',
 'distance_hcm_Indochina Plaza Hanoi Residences',
 'distance_hcm_Trung tâm thương mại Hà Nội Aqua Central',
 'distance_hcm_Trung tâm thương mại Hà Nội Center Point',
 'distance_hcm_Trung tâm thương mại Hà Nội Tràng Tiền Plaza ',
 'distance_hcm_Trung tâm thương mại Hà Nội Keangnam',
 'distance_hcm_Aeon Mall Long Biên',
 'distance_hcm_Vincom Bà Triệu',
 'distance_hcm_GO! Thăng Long',
 'distance_hcm_Tops Market The Garden',
 'distance_hcm_WinMart',
 'distance_hcm_Tops Market Lê Trọng Tấn',
 'distance_hcm_Lotte Mart Đống Đa',
 'distance_hcm_Lotte Mart Cầu Giấy',
 'distance_hcm_Công viên yên sở',
 'distance_hcm_Công viên thống nhất',
 'distance_hcm_Công viên thủ lệ',
 'distance_hcm_Công viên hòa bình',
 'distance_hcm_Công viên nước Hồ Tây',
 'distance_hcm_Công Viên Thiên Đường Bảo Sơn',
 'distance_hcm_Công viên Nghĩa Đô',
 'distance_hcm_Vườn hoa Lý Thái Tổ',
 'distance_hcm_Vườn hoa Lênin',
 'distance_hcm_Công viên Văn hóa Đống Đa',
 'distance_hcm_Vườn Hoa Pasteur',
 'distance_hcm_Hồ Ngọc Khánh',
 'distance_hcm_Hồ Gươm',
 'distance_hcm_Hồ Tây 1',
 'distance_hcm_Hồ Tây 2',
 'distance_hcm_Hồ Tây 3',
 'distance_hcm_Hồ Tây 4',
 'distance_hcm_Hồ Tây 5',
 'distance_hcm_Hồ Tây 6',
 'distance_hcm_Hồ Tây 7',
 'distance_hcm_Hồ Tây 8',
 'distance_hcm_Phố Lê Thái Tổ',
 'distance_hcm_Phố Bảo Khánh',
 'distance_hcm_Phố Hàng Đào',
 'distance_hcm_Hàng Khay',
 'distance_hcm_Hàng Trống',
 'distance_hcm_Hàng Hành',
 'distance_hcm_Phố Hai Bà Trưng',
 'distance_hcm_Phố Lý Thường Kiệt',
 'distance_hcm_Phố Bà Triệu',
 'distance_hcm_Hàng Bài',
 'distance_hcm_Phố Hai Bà Trưng(Hoàn Kiếm)',
 'distance_district']

def get_distance_feature(city, candidate_lat, candidate_lon, district_lat, district_lon):
    obj = dict()
    for i in range(len(best_arr)):
        lat, lon = best_arr[i][0], best_arr[i][1]
        name = best_arr[i][2]
        city_str = 'hn' if best_arr[i][3] == 0 else 'hcm'

        obj[f'distance_{city_str}_{name}'] = distance(candidate_lat, candidate_lon, lat, lon, city, best_arr[i][3])

    obj['distance_district'] = distance_func(candidate_lat, candidate_lon, district_lat, district_lon)

    for c in distance_cols:
        if obj[c] < 0:
            obj[c] = None
        else:
            obj[c] = math.log(obj[c])

    return obj


