import datetime
import json
import re

from datetime import datetime

with open('./streets.json', 'r') as f:
   streets = json.load(f)
def search_street(location):
   for street in streets:
      if street['STREET'].lower() in location and street['DISTRICT'].lower() in location:
         if street['WARD'].lower() in location:
            return street

   return None

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
   date = datetime.datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
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
            fullAddress = fullAddress.lower()
            search = search_street(fullAddress)
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
               "crawlInfo": { "id": a['code'], "source" : 's10','time' : time(a), 'db_create_time': datetime.now()}}


   return data_merge

