import os
import sys
import hashlib
import uuid
import time

# ========== ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ==========
try:
    ottai_token = str(os.environ['OTTAI_TOKEN'])
except KeyError:
    sys.exit("OTTAI_TOKEN required. Pass it as an Environment Variable.")

try:
    ottai_base_url = str(os.environ['OTTAI_BASE_URL'])
except KeyError:
    # Значение по умолчанию для обратной совместимости
    ottai_base_url = "https://seas.ottai.com"

try:
    ns_url = str(os.environ['NS_URL'])
except KeyError:
    sys.exit("NS_URL required. Pass it as an Environment Variable.")

try:
    ns_api_secret = str(os.environ['NS_API_SECRET'])
except KeyError:
    sys.exit("NS_API_SECRET required. Pass it as an Environment Variable.")

try:
    HOURS_AGO = int(os.environ['HOURS_AGO'])
except KeyError:
    sys.exit("HOURS_AGO required. Pass it as an Environment Variable.")

# ========== КОНСТАНТЫ ==========
ns_uploder = "Ottai-Nightscout-Uploader"
ns_unit_convert = 18.018

# ========== ФУНКЦИИ ДЛЯ ЗАГОЛОВКОВ ==========
def get_hash_SHA1(data):
    """Хеширование для Nightscout API secret"""
    hash_object = hashlib.sha1(data.encode())
    return hash_object.hexdigest()

def generate_trace_id():
    """Генерация уникального trace ID"""
    return str(uuid.uuid4())

def generate_timestamp():
    """Генерация timestamp в миллисекундах"""
    return int(time.time() * 1000)

# ========== ЗАГОЛОВКИ NIGHTSCOUT ==========
ns_header = {
    "api-secret": get_hash_SHA1(ns_api_secret),
    "Content-Type": "application/json",
    "Accept": "application/json",
}

# ========== БАЗОВЫЕ ЗАГОЛОВКИ OTTAI (ТОЛЬКО ОБЯЗАТЕЛЬНЫЕ) ==========
common_ottai_headers = {
    "authorization": ottai_token,
    "country": "RU",
    "language": "ru",
    "timezone": "10800",
    "region": "RU",
    "versionCode": "254632",  # Добавлен по требованию
}

# ========== ЗАГОЛОВКИ ДЛЯ ЗАПРОСА linkQueryList ==========
ottai_header_one_entries = common_ottai_headers.copy()

# ========== ЗАГОЛОВКИ ДЛЯ ЗАПРОСА queryMonitorBase ==========
# Инициализируем как None, будет установлено при первом вызове
ottai_header_array_entries = None

def init_ottai_headers():
    """Инициализация заголовков для запроса массива записей"""
    global ottai_header_array_entries
    
    headers = common_ottai_headers.copy()
    headers.update({
        "traceid": generate_trace_id(),
        "timestamp": str(generate_timestamp()),
        "deviceid": "beecbab9f9f9f889cd62d68846ed67f0",
    })
    
    ottai_header_array_entries = headers
    print(f"[DEBUG] Заголовки queryMonitorBase инициализированы. Ключи: {list(headers.keys())}")