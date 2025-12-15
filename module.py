from setup import *
import requests
import json
import datetime
from datetime import timedelta
import traceback
import concurrent.futures
import threading

# Импортируем необходимые компоненты из setup
from setup import (
    HOURS_AGO, NS_UNIT_CONVERT,
    get_common_ottai_headers,
    OTTAI_BASE_URL,
    get_nightscout_config_by_email, extract_clean_email, normalize_email_key,
    get_all_nightscout_configs,
    get_hash_SHA1
)

# ========== КОНСТАНТЫ И КЭШ ==========
REQUEST_TIMEOUT = 15
MAX_WORKERS = 3  # Максимальное количество потоков для параллельной обработки
BATCH_SIZE = 50  # Размер пачки для отправки в Nightscout

# Кэш для пользователей (обновляется каждые 5 минут)
_user_cache = {
    'data': None,
    'timestamp': 0,
    'lock': threading.Lock()
}

# Кэш для соединений с Nightscout
_connection_cache = {}

# ========== ОПТИМИЗИРОВАННЫЕ ФУНКЦИИ ==========
def convert_mmoll_to_mgdl(x):
    """Конвертация ммоль/л в мг/дл (оптимизированная)"""
    try:
        return int(float(x) * NS_UNIT_CONVERT + 0.5)  # Более быстрый round
    except (TypeError, ValueError):
        return 0

def get_all_users_from_ottai_cached(force_refresh=False):
    """
    Получение списка всех пользователей из Ottai с кэшированием
    """
    current_time = time.time()
    
    # Проверяем кэш (актуален 5 минут)
    if not force_refresh and _user_cache['data'] is not None:
        if current_time - _user_cache['timestamp'] < 300:  # 5 минут
            print(f"[INFO] Используем кэшированный список пользователей")
            return _user_cache['data']
    
    with _user_cache['lock']:
        # Проверяем еще раз после получения блокировки
        if not force_refresh and _user_cache['data'] is not None:
            if current_time - _user_cache['timestamp'] < 300:
                return _user_cache['data']
        
        # Получаем свежие данные
        users = _get_all_users_from_ottai_raw()
        
        # Обновляем кэш
        _user_cache['data'] = users
        _user_cache['timestamp'] = current_time
        
        return users

def _get_all_users_from_ottai_raw():
    """
    Получение списка всех пользователей из Ottai (без кэширования)
    """
    try:
        url = f"{OTTAI_BASE_URL}/link/application/app/tagFromInviteLink/linkQueryList/v2"
        
        headers = get_common_ottai_headers()
        headers['content-length'] = '0'
        
        print(f"[INFO] Запрос списка пользователей из Ottai...")
        
        response = requests.post(url, headers=headers, timeout=REQUEST_TIMEOUT)
        
        if response.status_code != 200:
            print(f"[ERROR] Ошибка запроса пользователей: {response.status_code}")
            return []
        
        data = response.json()
        users = []
        
        if 'data' in data and isinstance(data['data'], list):
            for user_item in data['data']:
                email = None
                
                # Быстрый поиск email в возможных полях
                for field in ['fromUserEmail', 'remark', 'email', 'userEmail']:
                    if field in user_item and user_item[field]:
                        email = user_item[field].strip()
                        break
                
                user_id = user_item.get('fromUserId') or user_item.get('id')
                
                if email and user_id:
                    users.append({
                        'email': email,
                        'fromUserId': user_id,
                        'raw_data': user_item
                    })
        
        print(f"[INFO] Найдено пользователей: {len(users)}")
        return users
        
    except requests.exceptions.Timeout:
        print(f"[ERROR] Таймаут при запросе пользователей")
        return []
    except Exception as e:
        print(f"[ERROR] Ошибка при получении пользователей: {str(e)}")
        return []

def create_user_config(user_email, from_user_id):
    """
    Создание конфигурации пользователя (оптимизированная)
    """
    ns_url, ns_secret = get_nightscout_config_by_email(user_email)
    
    if not ns_url or not ns_secret:
        return None
    
    config_key = normalize_email_key(user_email) or f"user_{from_user_id}"
    
    user_config = {
        'email': user_email,
        'from_user_id': from_user_id,
        'ns_url': ns_url.rstrip('/'),
        'ns_secret': ns_secret,
        'config_key': config_key,
        'ns_uploder': f"Ottai-{config_key}"
    }
    
    # Создаем заголовки Nightscout
    user_config['ns_header'] = {
        "api-secret": get_hash_SHA1(ns_secret),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    
    # Создаем заголовки Ottai для этого пользователя
    user_config['ottai_headers'] = get_common_ottai_headers()
    
    return user_config

def check_nightscout_connection_cached(user_config):
    """
    Проверка соединения с Nightscout (с кэшированием)
    """
    cache_key = f"{user_config['email']}_connection"
    current_time = time.time()
    
    # Проверяем кэш (актуален 1 минута)
    if cache_key in _connection_cache:
        cached_result, timestamp = _connection_cache[cache_key]
        if current_time - timestamp < 60:  # 1 минута
            return cached_result
    
    # Проверяем соединение
    result = _check_nightscout_connection_raw(user_config)
    
    # Обновляем кэш
    _connection_cache[cache_key] = (result, current_time)
    
    return result

def _check_nightscout_connection_raw(user_config):
    """
    Проверка соединения с Nightscout (без кэширования)
    """
    try:
        base_url = user_config['ns_url']
        url = f"{base_url}/api/v1/status"
        
        response = requests.get(url, headers=user_config['ns_header'], timeout=10)
        
        return response.status_code == 200
        
    except Exception:
        return False

def get_last_entry_date_fast(user_config):
    """
    Быстрое получение даты последней записи из Nightscout
    """
    try:
        base_url = user_config['ns_url']
        
        # Пробуем быстрый endpoint
        endpoints = [
            f"{base_url}/api/v1/entries.json?count=1",
            f"{base_url}/api/v1/entries/sgv.json?count=1"
        ]
        
        for url in endpoints:
            try:
                response = requests.get(url, headers=user_config['ns_header'], timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data and isinstance(data, list) and len(data) > 0 and 'date' in data[0]:
                        return data[0]['date']
            except:
                continue
        
        return None
        
    except Exception:
        return None

def get_ottai_data_batch(user_config, start_time, end_time):
    """
    Получение данных из Ottai пакетами
    """
    try:
        url = f"{OTTAI_BASE_URL}/link/application/search/tag/queryMonitorBase"
        
        params = {
            'fromUserId': user_config['from_user_id'],
            'isOpen': 0,
            'startTime': start_time,
            'endTime': end_time
        }
        
        response = requests.get(url, 
                              headers=user_config['ottai_headers'], 
                              params=params,
                              timeout=REQUEST_TIMEOUT)
        
        if response.status_code != 200:
            return []
        
        data = response.json()
        
        # Извлекаем curveList из разных возможных мест
        curve_list = None
        if 'data' in data and isinstance(data['data'], dict) and 'curveList' in data['data']:
            curve_list = data['data']['curveList']
        elif 'curveList' in data and isinstance(data['curveList'], list):
            curve_list = data['curveList']
        
        return curve_list or []
        
    except Exception as e:
        print(f"[ERROR] Ошибка при загрузке данных Ottai: {str(e)}")
        return []

def prepare_nightscout_entries(curve_list, user_config):
    """
    Подготовка записей для Nightscout (пакетная обработка)
    """
    entries = []
    
    for item in curve_list:
        try:
            if 'adjustGlucose' not in item or 'monitorTime' not in item:
                continue
            
            glucose = float(item['adjustGlucose'])
            timestamp = int(item['monitorTime'])
            
            entry = {
                "type": "sgv",
                "sgv": convert_mmoll_to_mgdl(glucose),
                "direction": "Flat",
                "device": user_config['ns_uploder'],
                "date": timestamp,
                "dateString": datetime.datetime.utcfromtimestamp(timestamp/1000).isoformat(timespec='milliseconds') + "Z"
            }
            
            entries.append(entry)
        except Exception:
            continue
    
    return entries

def send_to_nightscout_batch(user_config, entries):
    """
    Отправка записей в Nightscout пачками
    """
    if not entries:
        return 0
    
    base_url = user_config['ns_url']
    url = f"{base_url}/api/v1/entries"
    
    successful = 0
    
    # Разбиваем на пачки по BATCH_SIZE
    for i in range(0, len(entries), BATCH_SIZE):
        batch = entries[i:i + BATCH_SIZE]
        
        try:
            response = requests.post(url, 
                                   headers=user_config['ns_header'], 
                                   json=batch,
                                   timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 200:
                successful += len(batch)
            else:
                print(f"[ERROR] Ошибка при отправке пакета: {response.status_code}")
        except Exception:
            continue
    
    return successful

def process_user_data_optimized(user_config):
    """
    Оптимизированная обработка данных пользователя
    """
    print(f"\n[USER] {user_config['email']}")
    
    # 1. Проверяем соединение с Nightscout (с кэшированием)
    if not check_nightscout_connection_cached(user_config):
        print(f"  ❌ Nightscout недоступен")
        return 0
    
    # 2. Получаем последнюю запись из Nightscout
    last_ns_date = get_last_entry_date_fast(user_config)
    
    if last_ns_date:
        start_time = last_ns_date + 1
        print(f"  📊 Продолжаем с последней записи")
    else:
        start_time = int((datetime.datetime.now() - timedelta(hours=HOURS_AGO)).timestamp() * 1000)
        print(f"  📊 Загружаем за {HOURS_AGO} часов")
    
    current_time = int(datetime.datetime.now().timestamp() * 1000)
    
    if start_time >= current_time:
        print(f"  ℹ️ Нет новых данных")
        return 0
    
    # 3. Получаем данные из Ottai
    curve_list = get_ottai_data_batch(user_config, start_time, current_time)
    
    if not curve_list:
        print(f"  ℹ️ Нет данных в Ottai")
        return 0
    
    print(f"  📥 Получено {len(curve_list)} записей из Ottai")
    
    # 4. Подготавливаем записи для Nightscout
    entries = prepare_nightscout_entries(curve_list, user_config)
    
    if not entries:
        print(f"  ℹ️ Нет записей для обработки")
        return 0
    
    # 5. Отправляем пачками в Nightscout
    successful = send_to_nightscout_batch(user_config, entries)
    
    if successful > 0:
        print(f"  ✅ Отправлено {successful} записей в Nightscout")
    else:
        print(f"  ❌ Не удалось отправить записи")
    
    return successful

def process_user_wrapper(user_info):
    """
    Обертка для обработки пользователя в потоке
    """
    user_config = create_user_config(user_info['email'], user_info['fromUserId'])
    
    if not user_config:
        return 0
    
    try:
        return process_user_data_optimized(user_config)
    except Exception as e:
        print(f"[ERROR] Ошибка при обработке {user_info['email']}: {str(e)}")
        return 0

def process_all_users_optimized():
    """
    Оптимизированная обработка всех пользователей
    """
    print("\n" + "="*60)
    print("🚀 НАЧАЛО ОБРАБОТКИ (ОПТИМИЗИРОВАННОЙ)")
    print("="*60)
    
    # Получаем пользователей с кэшированием
    all_users = get_all_users_from_ottai_cached()
    
    if not all_users:
        print("❌ Не удалось получить пользователей из Ottai")
        return
    
    # Фильтруем настроенных пользователей
    configured_users = []
    for user in all_users:
        email = extract_clean_email(user['email']) or user['email']
        ns_url, ns_secret = get_nightscout_config_by_email(email)
        if ns_url and ns_secret:
            configured_users.append({
                'email': email,
                'fromUserId': user['fromUserId']
            })
    
    if not configured_users:
        print("❌ Нет настроенных пользователей")
        print("\n💡 Настройте переменные окружения NS_URL__ и NS_SECRET__")
        return
    
    print(f"👥 Пользователей к обработке: {len(configured_users)}")
    
    # Параллельная обработка пользователей
    total_successful = 0
    
    # Используем ThreadPoolExecutor для параллельной обработки
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Запускаем обработку каждого пользователя
        futures = []
        for user_info in configured_users:
            future = executor.submit(process_user_wrapper, user_info)
            futures.append(future)
        
        # Собираем результаты
        for future in concurrent.futures.as_completed(futures):
            total_successful += future.result()
    
    # Очищаем устаревшие кэши
    _cleanup_old_cache()
    
    print("\n" + "="*60)
    print(f"📊 ИТОГ: Успешно обработано {total_successful} записей")
    print("="*60)

def _cleanup_old_cache():
    """Очистка устаревших кэшей"""
    current_time = time.time()
    keys_to_remove = []
    
    for key, (_, timestamp) in _connection_cache.items():
        if current_time - timestamp > 300:  # 5 минут
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        del _connection_cache[key]
    
    # Очищаем кэш пользователей если старше 10 минут
    if current_time - _user_cache['timestamp'] > 600:  # 10 минут
        _user_cache['data'] = None
        _user_cache['timestamp'] = 0