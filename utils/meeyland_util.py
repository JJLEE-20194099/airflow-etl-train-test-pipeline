import json
import re

from datetime import datetime
import time as ti

import re
from bs4 import BeautifulSoup
import json
from unidecode import unidecode

from tqdm import tqdm
import regex as re

uniChars = "àáảãạâầấẩẫậăằắẳẵặèéẻẽẹêềếểễệđìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵÀÁẢÃẠÂẦẤẨẪẬĂẰẮẲẴẶÈÉẺẼẸÊỀẾỂỄỆĐÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴÂĂĐÔƠƯ"
unsignChars = "aaaaaaaaaaaaaaaaaeeeeeeeeeeediiiiiooooooooooooooooouuuuuuuuuuuyyyyyAAAAAAAAAAAAAAAAAEEEEEEEEEEEDIIIOOOOOOOOOOOOOOOOOOOUUUUUUUUUUUYYYYYAADOOU"


def loaddicchar():
    dic = {}
    char1252 = 'à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ'.split(
        '|')
    charutf8 = "à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ".split(
        '|')
    for i in range(len(char1252)):
        dic[char1252[i]] = charutf8[i]
    return dic
bang_nguyen_am = [['a', 'à', 'á', 'ả', 'ã', 'ạ', 'a'],
                  ['ă', 'ằ', 'ắ', 'ẳ', 'ẵ', 'ặ', 'aw'],
                  ['â', 'ầ', 'ấ', 'ẩ', 'ẫ', 'ậ', 'aa'],
                  ['e', 'è', 'é', 'ẻ', 'ẽ', 'ẹ', 'e'],
                  ['ê', 'ề', 'ế', 'ể', 'ễ', 'ệ', 'ee'],
                  ['i', 'ì', 'í', 'ỉ', 'ĩ', 'ị', 'i'],
                  ['o', 'ò', 'ó', 'ỏ', 'õ', 'ọ', 'o'],
                  ['ô', 'ồ', 'ố', 'ổ', 'ỗ', 'ộ', 'oo'],
                  ['ơ', 'ờ', 'ớ', 'ở', 'ỡ', 'ợ', 'ow'],
                  ['u', 'ù', 'ú', 'ủ', 'ũ', 'ụ', 'u'],
                  ['ư', 'ừ', 'ứ', 'ử', 'ữ', 'ự', 'uw'],
                  ['y', 'ỳ', 'ý', 'ỷ', 'ỹ', 'ỵ', 'y']]
bang_ky_tu_dau = ['', 'f', 's', 'r', 'x', 'j']

nguyen_am_to_ids = {}

for i in range(len(bang_nguyen_am)):
    for j in range(len(bang_nguyen_am[i]) - 1):
        nguyen_am_to_ids[bang_nguyen_am[i][j]] = (i, j)

dicchar = loaddicchar()
def chuan_hoa_dau_tu_tieng_viet(word):
    if not is_valid_vietnam_word(word):
        return word

    chars = list(word)
    dau_cau = 0
    nguyen_am_index = []
    qu_or_gi = False
    for index, char in enumerate(chars):
        x, y = nguyen_am_to_ids.get(char, (-1, -1))
        if x == -1:
            continue
        elif x == 9:  # check qu
            if index != 0 and chars[index - 1] == 'q':
                chars[index] = 'u'
                qu_or_gi = True
        elif x == 5:  # check gi
            if index != 0 and chars[index - 1] == 'g':
                chars[index] = 'i'
                qu_or_gi = True
        if y != 0:
            dau_cau = y
            chars[index] = bang_nguyen_am[x][0]
        if not qu_or_gi or index != 1:
            nguyen_am_index.append(index)
    if len(nguyen_am_index) < 2:
        if qu_or_gi:
            if len(chars) == 2:
                x, y = nguyen_am_to_ids.get(chars[1])
                chars[1] = bang_nguyen_am[x][dau_cau]
            else:
                x, y = nguyen_am_to_ids.get(chars[2], (-1, -1))
                if x != -1:
                    chars[2] = bang_nguyen_am[x][dau_cau]
                else:
                    chars[1] = bang_nguyen_am[5][dau_cau] if chars[1] == 'i' else bang_nguyen_am[9][dau_cau]
            return ''.join(chars)
        return word

    for index in nguyen_am_index:
        x, y = nguyen_am_to_ids[chars[index]]
        if x == 4 or x == 8:  # ê, ơ
            chars[index] = bang_nguyen_am[x][dau_cau]
            # for index2 in nguyen_am_index:
            #     if index2 != index:
            #         x, y = nguyen_am_to_ids[chars[index]]
            #         chars[index2] = bang_nguyen_am[x][0]
            return ''.join(chars)

    if len(nguyen_am_index) == 2:
        if nguyen_am_index[-1] == len(chars) - 1:
            x, y = nguyen_am_to_ids[chars[nguyen_am_index[0]]]
            chars[nguyen_am_index[0]] = bang_nguyen_am[x][dau_cau]
            # x, y = nguyen_am_to_ids[chars[nguyen_am_index[1]]]
            # chars[nguyen_am_index[1]] = bang_nguyen_am[x][0]
        else:
            # x, y = nguyen_am_to_ids[chars[nguyen_am_index[0]]]
            # chars[nguyen_am_index[0]] = bang_nguyen_am[x][0]
            x, y = nguyen_am_to_ids[chars[nguyen_am_index[1]]]
            chars[nguyen_am_index[1]] = bang_nguyen_am[x][dau_cau]
    else:
        # x, y = nguyen_am_to_ids[chars[nguyen_am_index[0]]]
        # chars[nguyen_am_index[0]] = bang_nguyen_am[x][0]
        x, y = nguyen_am_to_ids[chars[nguyen_am_index[1]]]
        chars[nguyen_am_index[1]] = bang_nguyen_am[x][dau_cau]
        # x, y = nguyen_am_to_ids[chars[nguyen_am_index[2]]]
        # chars[nguyen_am_index[2]] = bang_nguyen_am[x][0]
    return ''.join(chars)


def is_valid_vietnam_word(word):
    chars = list(word)
    nguyen_am_index = -1
    for index, char in enumerate(chars):
        x, y = nguyen_am_to_ids.get(char, (-1, -1))
        if x != -1:
            if nguyen_am_index == -1:
                nguyen_am_index = index
            else:
                if index - nguyen_am_index != 1:
                    return False
                nguyen_am_index = index
    return True


def chuan_hoa_dau_cau_tieng_viet(sentence):

    sentence = sentence.lower()
    words = sentence.split()
    for index, word in enumerate(words):
        cw = re.sub(r'(^\p{P}*)([p{L}.]*\p{L}+)(\p{P}*$)', r'\1/\2/\3', word).split('/')
        # print(cw)
        if len(cw) == 3:
            cw[1] = chuan_hoa_dau_tu_tieng_viet(cw[1])
        words[index] = ''.join(cw)
    return ' '.join(words)
def covert_unicode(txt):
    return re.sub(
        r'à|á|ả|ã|ạ|ầ|ấ|ẩ|ẫ|ậ|ằ|ắ|ẳ|ẵ|ặ|è|é|ẻ|ẽ|ẹ|ề|ế|ể|ễ|ệ|ì|í|ỉ|ĩ|ị|ò|ó|ỏ|õ|ọ|ồ|ố|ổ|ỗ|ộ|ờ|ớ|ở|ỡ|ợ|ù|ú|ủ|ũ|ụ|ừ|ứ|ử|ữ|ự|ỳ|ý|ỷ|ỹ|ỵ|À|Á|Ả|Ã|Ạ|Ầ|Ấ|Ẩ|Ẫ|Ậ|Ằ|Ắ|Ẳ|Ẵ|Ặ|È|É|Ẻ|Ẽ|Ẹ|Ề|Ế|Ể|Ễ|Ệ|Ì|Í|Ỉ|Ĩ|Ị|Ò|Ó|Ỏ|Õ|Ọ|Ồ|Ố|Ổ|Ỗ|Ộ|Ờ|Ớ|Ở|Ỡ|Ợ|Ù|Ú|Ủ|Ũ|Ụ|Ừ|Ứ|Ử|Ữ|Ự|Ỳ|Ý|Ỷ|Ỹ|Ỵ',
        lambda x: dicchar[x.group()], txt)

def preprocess_text(text):
    try:
        text = text.lower()
        text = covert_unicode(text)
        text = chuan_hoa_dau_cau_tieng_viet(text)
        return text
    except:
        return None



hcm_word = preprocess_text('hồ chí minh')
hn_word = preprocess_text('hà nội')

import difflib

with open('./streets.json', 'r') as f:
   streets = json.load(f)

locationql = [f'{item["STREET"].lower()} {item["DISTRICT"].lower()} {item["CITY"].lower()}' for item in tqdm(streets)]

def searchFloor(a):
   bio = a['content'].lower()
   if 'tầng' in bio:
      # tìm chữ số đứng trước chữ tầng
      floor = re.findall(r'\d+(?= tầng)', bio)
      if len(floor) > 0:
         return int(floor[0])
      else:
         bio = a['title'].lower()
         if 'tầng' in bio:
            # tìm chữ số đứng trước chữ tầng
            floor = re.findall(r'\d+(?= tầng)', bio)
            if len(floor) > 0:
               return int(floor[0])
   return 1

def certificateOfLandUseRight(a):
   return { "certificateStatus": "yes", "media": [] }

def time(a):
   date = a['publishedDate']
   # str = '2021-05-20T00:00:00.000Z' convert to '2021-05-20T00:00:00'
   date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
   return date.strftime('%Y-%m-%dT%H:%M:%S')

def propertyType(a):
   return 'houseForSale'

def propertyGeneralImage(a):
   data = []
   if 'images' not in a:
      return None
   images = a['images']
   images = [s3.upload_image_to_s3(image['url']) for image in images]
   for image in images:
      data.append({
                    "comment": [],
                    "status": "UNSELECTED",
                    "fileUrl": image,
                    "fileMimeType": "image/png",
                    "isThumbnail": False,
                })
   if len(data) > 0:
      data[0]['isThumbnail'] = True
   # thêm trường isThumbnail = True vào image đầu
   return data

def houseInfo(a):
   value = {   "numberOfFloors": searchFloor(a),
               "numberOfBedRooms": 0,
               "numberOfBathRooms": 0,
               "numberOfKitchens": 0,
               "numberOfLivingRooms": 0,
               "numberOfGarages": 0 }
   if 'bedroom' in a:
      if a['bedroom'] != None:
         value['numberOfBedRooms'] = a['bedroom']
   if 'bathroom' in a:
      if a['bathroom'] != None:
         value['numberOfBathRooms'] = a['bathroom']
   return value

def propertyBasicInfo(a):
   return 'residentialLand'

def accessibility(a):
   if 'wideRoad' in a:
      if a['wideRoad'] != None:
         wideRoad = a['wideRoad']
         if 'Ngõ ngách' in wideRoad:
            return 'theBottleNeckPoint'
         if 'Ngõ 1 ô tô' in wideRoad:
            return 'fitOneCarAndOneMotorbike'
         if 'Ngõ 2 ô tô' in wideRoad:
            return 'fitTwoCars'
         if 'Ngõ 3 ô tô' in wideRoad:
            return 'fitThreeCars'
         if 'Ngõ 4 ô tô' in wideRoad:
            return 'notInTheAlley'
   return 'notInTheAlley'


def search_street(query):
    best_matches = difflib.get_close_matches(query, locationql, n=1, cutoff=0.7)
    if best_matches:
        best_match = best_matches[0]
        best_match_index = locationql.index(best_match)
        best_location = streets[best_match_index]

        if (hcm_word in query or 'hcm' in query) and best_location['CITY'] == 'Hồ Chí Minh':
            return best_location

        if (hn_word in query or 'hn' in query) and best_location['CITY'] == "Hà Nội":
            return best_location

    else:
      return None

def address(a):
   data = {
   "addressDetails": '',
   "street": '',
   "ward": '',
   "district": '',
   "city": '',
   "country": "Việt Nam"
   }
   if 'locations' in a:
      if 'streetName' in a['locations'][0]:
         if a['locations'][0]['streetName'] != None:
            fullAddress = a['locations'][0]['streetName'] + ' ' + a['locations'][0]['districtName'] + ' ' + a['locations'][0]['cityName']
            # print(fullAddress)
            fullAddress = fullAddress.lower()
            search = search_street(fullAddress)
            # print("Search:", search)
            if search != None:
               data['street'] = search['STREET']
               data['ward'] = search['WARD']
               data['district'] = search['DISTRICT']
               data['city'] = search['CITY']
               lat = search['LAT']
               lng = search['LNG']
               return [data,lat,lng]
   return None

def typeOfRealEstate(a):
  if 'typeOfHouse' in a:
     typeOfHouse = a['typeOfHouse'][0].lower()
     if 'căn hộ' in typeOfHouse:
        return 'condominium'
     if 'liền kề' in typeOfHouse or 'shophouse' in typeOfHouse:
        return 'shophouse'
     if 'biệt thự' in typeOfHouse:
        return 'semiDetachedVilla'
     if 'đất' in typeOfHouse:
        return 'privateLand'
     if 'khác' in typeOfHouse:
        return 'otherTypesOfProperty'
  return 'privateProperty'

def frontWidth(a):
   if 'facade' in a:
      if a['facade'] != None:
         frontwidth_ = a['facade']
         return float(frontwidth_)
   return 0

def facade(a):
      if '2 mặt' in a['content'].lower() or '2 mặt' in a['title'].lower():
         return 'twoSideOpen'
      elif '3 mặt' in a['content'].lower() or '3 mặt' in a['title'].lower():
         return 'threeSideOpen'
      elif '4 mặt' in a['content'].lower() or '4 mặt' in a['title'].lower():
         return 'fourSideOpen'
      elif typeOfRealEstate(a) == 'townhouse':
         return 'twoSideOpen'
      elif typeOfRealEstate(a) == 'semiDetachedVilla':
         return 'twoSideOpen'
      elif typeOfRealEstate(a) == 'shophouse':
         return 'twoSideOpen'
      else:
         return 'oneSideOpen'

def houseDirection(a):
   try:
      if 'direction' in a:
         direction = a['direction'][0]
         if direction == 'Đông':
            return 'east'
         elif direction == 'Tây':
            return 'west'
         elif direction == 'Nam':
            return 'south'
         elif direction == 'Bắc':
            return 'north'
         elif direction == 'Đông Nam':
            return 'southeast'
         elif direction == 'Đông Bắc':
            return 'northeast'
         elif direction == 'Tây Nam':
            return 'southwest'
         elif direction == 'Tây Bắc':
            return 'northwest'
      return None
   except:
      return None

def amenities(a):
   return True

def name(a):
   if 'creator' in a:
      if 'name' in a['creator']:
         name_ = a['creator']['name']['first'] + ' ' + a['creator']['name']['last']
         return name_.strip()
   return ''

def numberPhone(a):
   if 'creator' in a:
      if 'phone' in a['creator']:
         return a['creator']['phone']
   return ''

def avatarUrl(a):
   if 'creator' in a:
      if 'avatar' in a['creator']:
         if a['creator']['avatar'] != '':
            return a['creator']['avatar']
         else:
            return None

def landSize(a):
   if 'area' in a:
      if a['area'] != None:
         return a['area']
   return 0

def price(a):
   try:
      if 'priceLabel' in a:
         if 'lượng' in a['priceLabel']:
            return None
         price_ = a['priceLabel'].replace('Tỷ','').replace(',','.')
         return float(price_)
   except:
      return None




def transferMeeyland(a):
   price_ = price(a)
   if price_ == None:
      return None

   propertyType_ = propertyType(a)
   if propertyType_ == 'houseForRent':
      return None
   # nếu address không có thì bỏ qua
   address_ = address(a)
   print("address_:", address_)
   if address_ == None:
      return None

   # nếu houseDirection không có thì bỏ qua
   houseDirection_ = houseDirection(a)
   data_merge = {
               "propertyType": propertyType_,
               "propertyStatus": "SURVEYING",
               "mediaInfo": {},
               "houseInfo": { "value": houseInfo(a)},
               "propertyBasicInfo": { "landType": { "value": propertyBasicInfo(a) },
                  "accessibility": { "value": accessibility(a) },
                  "distanceToNearestRoad": { "value": 0 },
                  "frontRoadWidth": { "value": 0 },
                  "address": { "value": address_[0] },
                  "description": { "value": a['content'] },
                  "geolocation": { "value":{
                        "latitude": { "value": address_[1] },
                        "longitude": { "value": address_[2]}}},
                  "typeOfRealEstate": { "value": typeOfRealEstate(a) },
                  "frontWidth": { "value": frontWidth(a) },
                  "endWidth": { "value": frontWidth(a) },
                  "facade": { "value": facade(a) },
                  "houseDirection": { "value": houseDirection_ },
                  "landSize": { "value": landSize(a) },
                  "contact": { "name": { "value": name(a) },
                     "role": "houseOwner",
                     "phoneNumber": { "value": numberPhone(a) },
                     "avatarUrl": { "value": avatarUrl(a) }},
                  "price": { "value": price_ },
                  "unitPrice": { "value": "billion" },
                  },
               "saleInfo": { },
               "crawlInfo": { "id": a['code'], "source" : 's10','time' : time(a), 'db_create_timestamp': ti.time()}}


   return data_merge

