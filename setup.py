import os
import sys
import hashlib
import uuid
import time
import re
import base64
import json

# ========== ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ==========
try:
    ottai_token = str(os.environ['OTTAI_TOKEN'])
except KeyError:
    sys.exit("OTTAI_TOKEN required. Pass it as an Environment Variable.")

try:
    ottai_base_url = str(os.environ['OTTAI_BASE_URL'])
except KeyError:
    ottai_base_url = "https://seas.ottai.com"

try:
    HOURS_AGO = int(os.environ['HOURS_AGO'])
except KeyError:
    sys.exit("HOURS_AGO required. Pass it as an Environment Variable.")

# Функция для извлечения customerid из JWT токена
def extract_customerid_from_token(token):
    """Извлечение customerid из JWT токена"""
    try:
        parts = token.split('.')
        if len(parts) >= 2:
            payload = parts[1]
            # Добавляем padding если нужно
            padding = 4 - len(payload) % 4
            if padding != 4:
                payload += '=' * padding
            decoded = base64.urlsafe_b64decode(payload)
            payload_data = json.loads(decoded)
            if 'userId' in payload_data:
                return payload_data['userId']
    except Exception:
        pass
    return None

# Пытаемся получить customerid из переменной окружения или токена
ottai_customerid = os.environ.get('OTTAI_CUSTOMER_ID')
if not ottai_customerid:
    ottai_customerid = extract_customerid_from_token(ottai_token) or ""

# ========== КОНСТАНТЫ ==========
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

def normalize_email_key(email):
    """
    Преобразование email в безопасный ключ для переменных окружения
    """
    if not email:
        return None
    
    username = email.split('@')[0].lower() if '@' in email else email.lower()
    safe_key = re.sub(r'[^a-z0-9_]', '_', username)
    return safe_key

def extract_clean_email(email_string):
    """
    Очистка email от лишних символов и извлечение чистого email
    """
    if not email_string:
        return None
    
    email_string = email_string.strip()
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    match = re.search(email_pattern, email_string)
    
    if match:
        return match.group(0).lower()
    
    if '@' in email_string:
        parts = email_string.split()
        for part in parts:
            if '@' in part:
                return part.lower()
    
    return email_string.lower() if email_string else None

def get_nightscout_config_by_email(user_email):
    """
    Получение конфигурации Nightscout для конкретного email
    """
    if not user_email:
        return None, None
    
    # Сначала пытаемся по полному email (без спецсимволов)
    full_key = re.sub(r'[^a-z0-9_]', '_', user_email.lower())
    ns_url_key = f"NS_URL__{full_key}"
    ns_secret_key = f"NS_SECRET__{full_key}"
    
    ns_url = os.environ.get(ns_url_key)
    ns_secret = os.environ.get(ns_secret_key)
    
    if ns_url and ns_secret:
        return ns_url.strip(), ns_secret.strip()
    
    # Если не нашли, пытаемся по имени пользователя (до @)
    config_key = normalize_email_key(user_email)
    if config_key:
        ns_url_key = f"NS_URL__{config_key}"
        ns_secret_key = f"NS_SECRET__{config_key}"
        
        ns_url = os.environ.get(ns_url_key)
        ns_secret = os.environ.get(ns_secret_key)
        
        if ns_url and ns_secret:
            return ns_url.strip(), ns_secret.strip()
    
    return None, None

def get_all_nightscout_configs():
    """
    Получение всех конфигураций Nightscout из переменных окружения
    Поддерживает оба формата: NS_SECRET__ и NS_API_SECRET__
    """
    configs = {}
    
    for env_key, ns_url in os.environ.items():
        if env_key.startswith("NS_URL__"):
            config_key = env_key[8:]  # Убираем "NS_URL__" префикс
            
            # Пробуем оба варианта ключей для секрета
            ns_secret = None
            
            # Вариант 1: NS_SECRET__ (основной формат)
            ns_secret_key = f"NS_SECRET__{config_key}"
            if ns_secret_key in os.environ:
                ns_secret = os.environ[ns_secret_key]
            
            # Вариант 2: NS_API_SECRET__ (альтернативный формат)
            if not ns_secret:
                ns_api_secret_key = f"NS_API_SECRET__{config_key}"
                if ns_api_secret_key in os.environ:
                    ns_secret = os.environ[ns_api_secret_key]
            
            if ns_url and ns_secret:
                configs[config_key] = (ns_url.strip(), ns_secret.strip())
    
    return configs

# ========== БАЗОВЫЕ ЗАГОЛОВКИ OTTAI (из вашего дампа) ==========
common_ottai_headers = {
    "authorization": ottai_token,
    "user-agent": "Dart/3.8 (dart:io)",
    "ua": "android",
    "deviceid": "Ottai Share:a:f:ee77b3508c1914df75fd5073c4450a9c",
    "accept-encoding": "gzip",
    "appname": "Ottai Share",
    "timestamp": "",  # Будет заполнено динамически
    "versioncode": "254921",
    "country": "RU",
    "traceid": "",  # Будет заполнено динамически
    "language": "ru",
    "timezone": "10800",
    "region": "RU",
    "packagename": "com.ottai.share",
    "host": "seas.ottai.com",
    "unit": "mmol_L",
    "timezonename": "MSK",
    "customerid": ottai_customerid,
    "versionname": "1.8.0",
}

# ========== ЗАГОЛОВКИ ДЛЯ ЗАПРОСА linkQueryList ==========
ottai_header_one_entries = common_ottai_headers.copy()

# ========== ЗАГОЛОВКИ ДЛЯ ЗАПРОСА queryMonitorBase ==========
def get_ottai_headers_for_user():
    """Создание заголовков для запроса массива записей"""
    headers = common_ottai_headers.copy()
    headers.update({
        "traceid": generate_trace_id(),
        "timestamp": str(generate_timestamp()),
    })
    return headers

# ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ==========
nightscout_configs = get_all_nightscout_configs()
user_configs = []