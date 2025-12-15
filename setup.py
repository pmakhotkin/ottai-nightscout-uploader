import os
import sys
import hashlib
import uuid
import time
import re
import base64
import json

# ========== ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ ==========
def load_config():
    """Загрузка конфигурации из переменных окружения"""
    config = {}
    
    try:
        config['ottai_token'] = str(os.environ['OTTAI_TOKEN'])
    except KeyError:
        sys.exit("OTTAI_TOKEN required. Pass it as an Environment Variable.")

    config['ottai_base_url'] = str(os.environ.get('OTTAI_BASE_URL', "https://seas.ottai.com"))
    
    try:
        config['hours_ago'] = int(os.environ['HOURS_AGO'])
    except KeyError:
        sys.exit("HOURS_AGO required. Pass it as an Environment Variable.")
    
    config['ottai_customerid'] = os.environ.get('OTTAI_CUSTOMER_ID', "")
    
    return config

CONFIG = load_config()

# ========== КОНСТАНТЫ ==========
NS_UNIT_CONVERT = 18.018

# ========== КЭШ ==========
_nightscout_config_cache = None

# ========== ФУНКЦИИ ДЛЯ ЗАГОЛОВКОВ ==========
def get_hash_SHA1(data):
    """Хеширование для Nightscout API secret"""
    return hashlib.sha1(data.encode()).hexdigest()

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
    
    # Быстрая нормализация через replace
    username = email.split('@')[0].lower() if '@' in email else email.lower()
    return re.sub(r'[^a-z0-9_]', '_', username)

def extract_clean_email(email_string):
    """
    Очистка email от лишних символов и извлечение чистого email
    """
    if not email_string:
        return None
    
    email_string = email_string.strip()
    
    # Быстрый поиск email
    match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', email_string)
    if match:
        return match.group(0).lower()
    
    # Если нет совпадения, пытаемся извлечь вручную
    if '@' in email_string:
        for part in email_string.split():
            if '@' in part:
                return part.lower()
    
    return email_string.lower()

def get_nightscout_config_by_email(user_email):
    """
    Получение конфигурации Nightscout для конкретного email (с кэшированием)
    """
    if not user_email:
        return None, None
    
    # Получаем все конфигурации
    configs = get_all_nightscout_configs()
    
    # Ищем конфигурацию по email
    user_key = normalize_email_key(user_email)
    if user_key and user_key in configs:
        return configs[user_key]
    
    return None, None

def get_all_nightscout_configs():
    """
    Получение всех конфигураций Nightscout из переменных окружения (с кэшированием)
    """
    global _nightscout_config_cache
    
    # Если есть кэш, возвращаем его
    if _nightscout_config_cache is not None:
        return _nightscout_config_cache
    
    configs = {}
    env_vars = os.environ
    
    # Быстрый поиск по переменным окружения
    for key, value in env_vars.items():
        if key.startswith("NS_URL__"):
            config_key = key[8:]  # Убираем "NS_URL__"
            
            if config_key:
                # Ищем секрет
                secret = env_vars.get(f"NS_SECRET__{config_key}")
                if not secret:
                    secret = env_vars.get(f"NS_API_SECRET__{config_key}")
                
                if secret:
                    configs[config_key] = (value.strip(), secret.strip())
    
    # Кэшируем результат
    _nightscout_config_cache = configs
    return configs

# ========== БАЗОВЫЕ ЗАГОЛОВКИ OTTAI ==========
def get_common_ottai_headers():
    """Создание базовых заголовков Ottai"""
    return {
        "authorization": CONFIG['ottai_token'],
        "user-agent": "Dart/3.8 (dart:io)",
        "ua": "android",
        "deviceid": "Ottai Share:a:f:ee77b3508c1914df75fd5073c4450a9c",
        "accept-encoding": "gzip",
        "appname": "Ottai Share",
        "timestamp": str(generate_timestamp()),
        "versioncode": "254921",
        "country": "RU",
        "traceid": generate_trace_id(),
        "language": "ru",
        "timezone": "10800",
        "region": "RU",
        "packagename": "com.ottai.share",
        "host": "seas.ottai.com",
        "unit": "mmol_L",
        "timezonename": "MSK",
        "customerid": CONFIG['ottai_customerid'],
        "versionname": "1.8.0",
    }

# ========== ЭКСПОРТ ПЕРЕМЕННЫХ ==========
OTTAI_TOKEN = CONFIG['ottai_token']
OTTAI_BASE_URL = CONFIG['ottai_base_url']
HOURS_AGO = CONFIG['hours_ago']
OTTAI_CUSTOMERID = CONFIG['ottai_customerid']