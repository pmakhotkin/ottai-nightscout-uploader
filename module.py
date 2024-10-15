from setup import *
import requests
import json
import datetime
from datetime import timedelta
#from datetime import datetime

def convert_mmoll_to_mgdl(x):
    return round(x*ns_unit_convert)

# return last entry date. (Slice allows searching for modal times of day across days and months.)
def get_last_entry_date():
    r=requests.get(ns_url+"/api/v1/slice/entries/dateString/sgv/.*/.*?count=1", headers=ns_header)
    try:
        data = r.json()
        print("Nightscout request", r.status_code , r.reason)
        if data == []:
            print("no data")
            return 0
        else:
            print("Last entry date:" , data[0]["date"] ,"(GMT",datetime.datetime.utcfromtimestamp(data[0]["date"]/1000),")")
            return data[0]["date"]
    except requests.JSONDecodeError:
        content_type = r.headers.get('Content-Type')
        print("Failed. Content Type " + content_type)

def convert_mgdl_to_mmoll(x):
    return round(x/ns_unit_convert, 1)

# process ottai data
def get_ottai_one_entry():
    try:
        r=requests.post("https://seas.ottai.com/link/application/app/tagFromInviteLink/linkQueryList/v2", headers=ottai_header_one_entries)
        data = r.json()
        print("ottai Response Status:" , r.status_code , r.reason)
        #pprint.pprint(r.json(), compact=True)
    except requests.JSONDecodeError:
        content_type = r.headers.get('Content-Type')
        print("Failed. Content Type " , content_type)
    return data

def get_fromUserId():
    try:
        r=requests.post("https://seas.ottai.com/link/application/app/tagFromInviteLink/linkQueryList/v2",headers=ottai_header_one_entries)
        data = r.json()
        print("Ottai get User Id Response Status:" , r.status_code , r.reason)
        return data['data'][0]['fromUserId']
    except requests.JSONDecodeError:
        e = r.get('msg')
        print(e)

def get_ottai_array_of_entries(lastDate = int(round((datetime.datetime.now() - timedelta(hours=5)).timestamp() * 1000))):
    fromUserId = get_fromUserId()
    currentDate = int(round(datetime.datetime.now().timestamp() * 1000))
    params = f'fromUserId={fromUserId}&startTime={lastDate}&endTime={currentDate}'
    try:
        r=requests.get("https://seas.ottai.com/link/application/search/tag/queryMonitorBase",headers=ottai_header_array_entries,params=params)
        data = r.json()
        print("Ottai get entries Response Status:" , r.status_code , r.reason)
        return data
    except requests.JSONDecodeError:
        e = r.get('msg')
        print(e)


# example of json to Nightscout
# {
#  "type": "sgv",
#  "sgv": 146,
#  "direction": "Flat",
#  "device": "Test-Uploader",
#  "date": 1725247634000,
#  "dateString": "2024-09-02T03:27:14.000Z"
# }

def process_json_data_prepare_json(data_ottai):
    dict_data =[]
    try:
       for item in data_ottai['data']['curveList']:
            entry_dict = {
            "type" : "sgv",
            "sgv" : convert_mmoll_to_mgdl(item['adjustGlucose']),
            "direction" : "FortyFiveUp",
            "device": ns_uploder,
            "date" : item['monitorTime'],
            "dateString": str(datetime.datetime.utcfromtimestamp(item['monitorTime']/1000).isoformat(timespec='milliseconds')+"Z")
            }
            dict_data.append(entry_dict)       
    except Exception as error:
        print("Error reading glucoseInfo from ottai:", error)
        return
    return dict_data

def process_json_data(data):
    print("Processing data...")
    try:
        dict_json_for_upload = process_json_data_prepare_json(data)
        for item in dict_json_for_upload:
            upload_json = json.loads(json.dumps(item))
            upload_entry(upload_json)

    except Exception as error:
        print("Error reading glucose data:", error)
    

def upload_entry(entries_json): #entries tpye = a list of dicts
    r=requests.post(ns_url+"/entries", headers = ns_header, json = entries_json)
    # 17:28
    if r.status_code == 200:
        os.system('cls' if os.name == 'nt' else 'clear')
        print("Nightscout POST request", r.status_code , r.reason)
        print(datetime.datetime.now())
        print("entries uploaded.")
    else:
        print("Nightscout POST request", r.status_code , r.reason, r.text)

def uploader_entries_by_period(ottai_data):
    try:
        for item in ottai_data['data']['curveList']:
           glucose = item['adjustGlucose']

    except Exception as error:
        print("Error reading ottai data by period", error)
# return query entry date. (Slice allows searching for modal times of day across days and months.)
def get_query_entry_date(query_date,header):
    r=requests.get(ns_url+"slice/entries/dateString/sgv/"+query_date+".*", headers=header)
    try:
        data = r.json()
        print(datetime.datetime.now() + " Nightscout request", r.status_code , r.reason)
        print("Last entry date" , data[0]["date"] ,"GMT",datetime.datetime.utcfromtimestamp(data[0]["date"]/1000))
    except requests.JSONDecodeError:
        content_type = r.headers.get('Content-Type')
        print("Failed. Content Type " + content_type)
    return data[0]["date"]

