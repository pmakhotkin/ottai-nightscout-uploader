import os
import sys
import hashlib

# Initilisation for local python script
# OTTAI_TOKEN = ""
# NS_URL = ""
# NS_API_SECRET= "" #api_secret

try:
    ottai_token = str(os.environ['OTTAI_TOKEN'])
except:
    sys.exit("OTTAI_TOKEN required. Pass it as an Environment Variable.")

    ottai_url_array_entries = "https://seas.ottai.com/link/application/search/tag/queryMonitorBase"


# Nightscout 
try:
    ns_url = str(os.environ['NS_URL'])
except:
    sys.exit("NS_URL required. Pass it as an Environment Variable.")

try:
    ns_api_secret = str(os.environ['NS_API_SECRET'])
except:
    sys.exit("NS_API_SECRET required. Pass it as an Environment Variable.")

# uploader initialisation
ns_uploder = "Ottai-Nightscout-Uploader"
ns_unit_convert = 18.018

def get_hash_SHA1(data):
    hash_object =hashlib.sha1(data.encode())
    hex_dig = hash_object.hexdigest()
    return hex_dig

# header initialisation
ns_header = {"api-secret": get_hash_SHA1(ns_api_secret),
             "Content-Type": "application/json",
             "Accept":"application/json",
             }

ottai_header_one_entries = {"authorization":ottai_token,
                "country": "RU",
                "language": "RU",
             }

ottai_header_array_entries = {"authorization":ottai_token,
                "country": "RU",
                "language": "RU",
                "timezone": "10800"
             }
