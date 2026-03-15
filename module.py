from setup import *
import requests
import json
import datetime
from datetime import timedelta
import traceback
import concurrent.futures
import threading
import urllib3
import time

# Подавляем предупреждения о SSL
if DISABLE_SSL_VERIFY:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Импортируем необходимые компоненты из setup
from setup import (
    HOURS_AGO, NS_UNIT_CONVERT,
    get_common_ottai_headers,
    OTTAI_BASE_URL, OTTAI_CUSTOMERID, DISABLE_SSL_VERIFY,
    get_nightscout_config_by_email, extract_clean_email, normalize_email_key,
    get_all_nightscout_configs, get_all_nightscout_configs_display,
    get_hash_SHA1
)

# ========== КОНСТАНТЫ И КЭШ ==========
REQUEST_TIMEOUT = 30
MAX_WORKERS = 3
BATCH_SIZE = 50

_user_cache = {
    'data': None,
    'timestamp': 0,
    'lock': threading.Lock()
}

_connection_cache = {}

# ========== ОПТИМИЗИРОВАННЫЕ ФУНКЦИИ ==========
def convert_mmoll_to_mgdl(x):
    """Конвертация ммоль/л в мг/дл"""
    try:
        return int(float(x) * NS_UNIT_CONVERT + 0.5)
    except (TypeError, ValueError):
        return 0

def get_session():
    """Создание HTTP сессии с настройками SSL"""
    session = requests.Session()
    
    if DISABLE_SSL_VERIFY:
        session.verify = False
    
    return session

def get_all_users_from_ottai_cached(force_refresh=False):
    """
    Получение списка всех пользователей из Ottai с кэшированием
    """
    current_time = time.time()
    
    if not force_refresh and _user_cache['data'] is not None:
        if current_time - _user_cache['timestamp'] < 300:
            print(f"[INFO] Используем кэшированный список пользователей")
            return _user_cache['data']
    
    with _user_cache['lock']:
        if not force_refresh and _user_cache['data'] is not None:
            if current_time - _user_cache['timestamp'] < 300:
                return _user_cache['data']
        
        users = _get_all_users_from_ottai_raw()
        
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
        
        session = get_session()
        response = session.post(url, headers=headers, timeout=REQUEST_TIMEOUT, verify=not DISABLE_SSL_VERIFY)
        
        if response.status_code != 200:
            print(f"[ERROR] Ошибка запроса пользователей: {response.status_code}")
            if response.text:
                print(f"[DEBUG] Тело ответа: {response.text[:500]}")
            return []
        
        data = response.json()
        
        users = []

        # Формат 1: список пользователей в data.data (Ottai multiuser)
        if 'data' in data and isinstance(data['data'], list):
            for user_item in data['data']:
                email = None
                for field in ['fromUserEmail', 'remark', 'email', 'userEmail']:
                    if field in user_item and user_item[field]:
                        email = user_item[field].strip()
                        break
                user_id = user_item.get('fromUserId') or user_item.get('id')
                user_name = user_item.get('userName') or ''
                if user_id:
                    users.append({
                        'email': email or '',
                        'fromUserId': user_id,
                        'userName': user_name,
                        'raw_data': user_item
                    })

        # Формат 2: одиночный fromUserId в корне ответа или в data.data (Syai)
        if not users:
            single_id = None
            if 'fromUserId' in data:
                single_id = data['fromUserId']
            elif 'data' in data and isinstance(data['data'], dict):
                single_id = data['data'].get('fromUserId') or data['data'].get('id')

            if single_id:
                users.append({
                    'email': '',
                    'fromUserId': str(single_id),
                    'userName': '',
                    'raw_data': data
                })

        # Fallback: OTTAI_CUSTOMER_ID из переменной окружения
        if not users and OTTAI_CUSTOMERID:
            print(f"[INFO] Список пользователей пуст, используем OTTAI_CUSTOMER_ID={OTTAI_CUSTOMERID}")
            users.append({
                'email': '',
                'fromUserId': OTTAI_CUSTOMERID,
                'userName': '',
                'raw_data': {}
            })

        print(f"[INFO] Найдено пользователей: {len(users)}")
        return users
        
    except requests.exceptions.Timeout:
        print(f"[ERROR] Таймаут при запросе пользователей")
        return []
    except requests.exceptions.SSLError as e:
        print(f"[ERROR] SSL ошибка: {e}")
        return []
    except Exception as e:
        print(f"[ERROR] Ошибка при получении пользователей: {str(e)}")
        traceback.print_exc()
        return []

def display_available_masters(all_users):
    """
    Отображение всех доступных мастеров
    """
    print("\n" + "="*80)
    print("ДОСТУПНЫЕ МАСТЕРЫ В OTTAI")
    print("="*80)
    
    if not all_users:
        print("❌ Нет доступных мастеров в Ottai")
        return []

    print(f"Всего мастеров в Ottai: {len(all_users)}")
    print("\nСписок мастеров:")
    print("-"*80)

    master_statuses = []

    for idx, user in enumerate(all_users, 1):
        email = user['email']
        user_id = user['fromUserId']
        user_name = user.get('userName', '')
        clean_email = extract_clean_email(email)

        ns_url, ns_secret = get_nightscout_config_by_email(clean_email or email, user_name, user_id)
        status = "✅ НАСТРОЕН" if ns_url and ns_secret else "❌ НЕ НАСТРОЕН"

        config_key = "—"
        if ns_url and ns_secret:
            config_key = normalize_email_key(clean_email or email) or user_name or str(user_id) or "unknown"
        
        master_statuses.append({
            'index': idx,
            'email': email,
            'clean_email': clean_email,
            'user_name': user_name,
            'user_id': user_id,
            'configured': bool(ns_url and ns_secret),
            'config_key': config_key
        })

        print(f"{idx:2d}. {email or f'(нет email, userName={user_name})'}")
        print(f"    ID: {user_id}")
        if user_name:
            print(f"    userName: {user_name}")
        print(f"    Статус: {status}")
        if ns_url and ns_secret:
            print(f"    Конфиг: {config_key}")
            print(f"    Nightscout URL: {ns_url[:50]}...")
        print()
    
    return master_statuses

def create_user_config(user_email, from_user_id, user_name=None):
    """
    Создание конфигурации пользователя
    """
    ns_url, ns_secret = get_nightscout_config_by_email(user_email, user_name, from_user_id)

    if not ns_url or not ns_secret:
        return None

    config_key = normalize_email_key(user_email) or user_name or str(from_user_id)
    
    user_config = {
        'email': user_email,
        'from_user_id': from_user_id,
        'ns_url': ns_url.rstrip('/'),
        'ns_secret': ns_secret,
        'config_key': config_key,
        'ns_uploder': f"Ottai-{config_key}",
        'session': get_session()
    }
    
    user_config['ns_header'] = {
        "api-secret": get_hash_SHA1(ns_secret),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    
    user_config['ottai_headers'] = get_common_ottai_headers()
    
    return user_config

def check_nightscout_connection_cached(user_config):
    """
    Проверка соединения с Nightscout (с кэшированием)
    """
    cache_key = f"{user_config['email']}_connection"
    current_time = time.time()
    
    if cache_key in _connection_cache:
        cached_result, timestamp = _connection_cache[cache_key]
        if current_time - timestamp < 60:
            return cached_result
    
    result = _check_nightscout_connection_raw(user_config)
    _connection_cache[cache_key] = (result, current_time)
    
    return result

def _check_nightscout_connection_raw(user_config):
    """
    Проверка соединения с Nightscout (без кэширования)
    """
    try:
        base_url = user_config['ns_url']
        url = f"{base_url}/api/v1/status"
        
        session = user_config['session']
        response = session.get(url, headers=user_config['ns_header'], timeout=10, verify=not DISABLE_SSL_VERIFY)
        
        return response.status_code == 200
        
    except requests.exceptions.SSLError as e:
        print(f"[WARNING] SSL ошибка при проверке Nightscout: {e}")
        try:
            session = user_config['session']
            response = session.get(url, headers=user_config['ns_header'], timeout=10, verify=False)
            return response.status_code == 200
        except:
            return False
    except Exception:
        return False

def get_ottai_data_batch(user_config, start_time, end_time):
    """
    Получение данных из Ottai пакетами (использует GET запрос)
    """
    try:
        url = f"{OTTAI_BASE_URL}/link/application/search/tag/queryMonitorBase"
        session = user_config['session']
        
        # Формируем параметры для GET-запроса
        params = {
            'fromUserId': user_config['from_user_id'],
            'isOpen': 0,
            'startTime': start_time,
            'endTime': end_time
        }
        
        print(f"[DEBUG] Запрос данных Ottai для {user_config['email']}")
        print(f"[DEBUG] Метод: GET")
        print(f"[DEBUG] URL: {url}")
        print(f"[DEBUG] Параметры: {json.dumps(params, indent=2)}")
        print(f"[DEBUG] Start time: {datetime.datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[DEBUG] End time: {datetime.datetime.fromtimestamp(end_time/1000).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[DEBUG] Time range: {(end_time - start_time) / 1000 / 60:.1f} минут")
        
        response = session.get(url, 
                             headers=user_config['ottai_headers'], 
                             params=params,
                             timeout=REQUEST_TIMEOUT,
                             verify=not DISABLE_SSL_VERIFY)
        
        print(f"[DEBUG] Статус ответа Ottai: {response.status_code}")
        
        if response.status_code != 200:
            print(f"[ERROR] Ошибка запроса Ottai: {response.status_code}")
            if response.text:
                print(f"[DEBUG] Тело ответа: {response.text[:500]}")
            return []
        
        data = response.json()
        print(f"[DEBUG] Успешно получен ответ от Ottai")
        print(f"[DEBUG] Структура ответа (первые 500 символов): {json.dumps(data, indent=2)[:500]}...")
        
        curve_list = None
        
        # Пробуем разные пути к данным
        if 'data' in data:
            if isinstance(data['data'], dict) and 'curveList' in data['data']:
                curve_list = data['data']['curveList']
            elif isinstance(data['data'], list):
                curve_list = data['data']
        elif 'curveList' in data and isinstance(data['curveList'], list):
            curve_list = data['curveList']
        elif isinstance(data, list):
            curve_list = data
        
        if curve_list:
            print(f"[DEBUG] Найдено записей в curveList: {len(curve_list)}")
            if len(curve_list) > 0:
                first_item = curve_list[0]
                print(f"[DEBUG] Первая запись: {json.dumps(first_item, indent=2)[:200]}...")
        else:
            print(f"[DEBUG] curveList не найден или пуст")
            print(f"[DEBUG] Полный ответ: {json.dumps(data, indent=2)}")
        
        return curve_list or []
        
    except requests.exceptions.SSLError:
        try:
            session = user_config['session']
            response = session.get(url, 
                                 headers=user_config['ottai_headers'], 
                                 params=params,
                                 timeout=REQUEST_TIMEOUT,
                                 verify=False)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and isinstance(data['data'], dict) and 'curveList' in data['data']:
                    return data['data']['curveList']
                elif 'curveList' in data and isinstance(data['curveList'], list):
                    return data['curveList']
        except Exception as e:
            print(f"[ERROR] SSL ошибка при загрузке данных Ottai: {e}")
        
        return []
    except Exception as e:
        print(f"[ERROR] Ошибка при загрузке данных Ottai: {str(e)}")
        traceback.print_exc()
        return []

def prepare_nightscout_entries(curve_list, user_config):
    """
    Подготовка записей для Nightscout
    """
    entries = []
    
    for item in curve_list:
        try:
            # Проверяем разные возможные ключи для глюкозы
            glucose_value = None
            glucose_keys = ['adjustGlucose', 'glucose', 'value', 'bgValue', 'sgv']
            
            for key in glucose_keys:
                if key in item and item[key] is not None:
                    glucose_value = item[key]
                    break
            
            # Проверяем разные возможные ключи для времени
            timestamp_value = None
            timestamp_keys = ['monitorTime', 'timestamp', 'date', 'time', 'created_at']
            
            for key in timestamp_keys:
                if key in item and item[key] is not None:
                    timestamp_value = item[key]
                    break
            
            if glucose_value is None or timestamp_value is None:
                continue
            
            glucose = float(glucose_value)
            timestamp = int(timestamp_value)
            
            entry = {
                "type": "sgv",
                "sgv": convert_mmoll_to_mgdl(glucose),
                "direction": "Flat",
                "device": user_config['ns_uploder'],
                "date": timestamp,
                "dateString": datetime.datetime.utcfromtimestamp(timestamp/1000).isoformat(timespec='milliseconds') + "Z"
            }
            
            # Пытаемся определить направление тренда
            if 'trend' in item:
                trend_map = {
                    'rising': 'DoubleUp',
                    'falling': 'DoubleDown',
                    'stable': 'Flat',
                    'DoubleUp': 'DoubleUp',
                    'DoubleDown': 'DoubleDown',
                    'SingleUp': 'SingleUp',
                    'SingleDown': 'SingleDown',
                    'FortyFiveUp': 'FortyFiveUp',
                    'FortyFiveDown': 'FortyFiveDown',
                    'Flat': 'Flat'
                }
                entry['direction'] = trend_map.get(item['trend'], 'Flat')
            elif 'direction' in item:
                entry['direction'] = item['direction']
            
            entries.append(entry)
            
        except Exception as e:
            print(f"[DEBUG] Ошибка обработки записи: {e}")
            continue
    
    print(f"[DEBUG] Подготовлено {len(entries)} записей для Nightscout")
    return entries

def send_to_nightscout_batch(user_config, entries):
    """
    Отправка записей в Nightscout пачками
    """
    if not entries:
        return 0
    
    base_url = user_config['ns_url']
    url = f"{base_url}/api/v1/entries"
    session = user_config['session']
    
    successful = 0
    
    # Отправляем по одной записи для лучшей отладки
    for i, entry in enumerate(entries):
        try:
            time_str = datetime.datetime.fromtimestamp(entry['date']/1000).strftime('%H:%M:%S')
            print(f"[DEBUG] Отправка записи {i+1}/{len(entries)}: {time_str}, глюкоза: {entry['sgv']}")
            
            response = session.post(url, 
                                   headers=user_config['ns_header'], 
                                   json=entry,
                                   timeout=REQUEST_TIMEOUT,
                                   verify=not DISABLE_SSL_VERIFY)
            
            if response.status_code == 200:
                successful += 1
                print(f"[DEBUG] ✅ Запись {time_str} отправлена успешно")
            else:
                print(f"[ERROR] Ошибка при отправке записи {time_str}: {response.status_code}")
                if response.text:
                    print(f"[DEBUG] Ответ Nightscout: {response.text[:200]}")
        except requests.exceptions.SSLError:
            try:
                response = session.post(url, 
                                       headers=user_config['ns_header'], 
                                       json=entry,
                                       timeout=REQUEST_TIMEOUT,
                                       verify=False)
                if response.status_code == 200:
                    successful += 1
                    print(f"[DEBUG] ✅ Запись {time_str} отправлена успешно (с отключенным SSL)")
                else:
                    print(f"[ERROR] Ошибка SSL при отправке записи {time_str}: {response.status_code}")
            except Exception as e:
                print(f"[ERROR] SSL ошибка при отправке в Nightscout: {e}")
        except Exception as e:
            print(f"[ERROR] Ошибка при отправке записи {time_str}: {e}")
    
    return successful

def process_user_data_optimized(user_config):
    """
    Оптимизированная обработка данных пользователя БЕЗ учета последней записи
    Просто загружаем данные за фиксированный период и отправляем их
    """
    print(f"\n[USER] {user_config['email']} (ID: {user_config['from_user_id']})")
    
    if not check_nightscout_connection_cached(user_config):
        print(f"  ❌ Nightscout недоступен")
        return 0
    
    # Фиксированный временной диапазон - за HOURS_AGO часов
    end_time = int(datetime.datetime.now().timestamp() * 1000)
    start_time = int((datetime.datetime.now() - timedelta(hours=HOURS_AGO)).timestamp() * 1000)
    
    start_str = datetime.datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d %H:%M:%S')
    end_str = datetime.datetime.fromtimestamp(end_time/1000).strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"  📊 Загружаем данные за {HOURS_AGO} часов")
    print(f"     Начало: {start_str}")
    print(f"     Конец:  {end_str}")
    
    if start_time >= end_time:
        print(f"  ℹ️ Некорректный временной диапазон")
        return 0
    
    # Получаем данные из Ottai
    curve_list = get_ottai_data_batch(user_config, start_time, end_time)
    
    if not curve_list:
        print(f"  ℹ️ Нет данных в Ottai")
        return 0
    
    print(f"  📥 Получено {len(curve_list)} записей из Ottai")
    
    # Подготавливаем записи для Nightscout
    entries = prepare_nightscout_entries(curve_list, user_config)
    
    if not entries:
        print(f"  ℹ️ Нет записей для обработки")
        return 0
    
    print(f"  📊 Диапазон данных в Ottai:")
    if entries:
        first_time = datetime.datetime.fromtimestamp(entries[0]['date']/1000).strftime('%H:%M:%S')
        last_time = datetime.datetime.fromtimestamp(entries[-1]['date']/1000).strftime('%H:%M:%S')
        print(f"     Первая запись: {first_time}")
        print(f"     Последняя запись: {last_time}")
    
    # Отправляем записи в Nightscout
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
    user_config = create_user_config(user_info['email'], user_info['fromUserId'], user_info.get('userName'))
    
    if not user_config:
        print(f"[WARNING] Пользователь {user_info['email']} не настроен")
        return 0
    
    try:
        return process_user_data_optimized(user_config)
    except Exception as e:
        print(f"[ERROR] Ошибка при обработке {user_info['email']}: {str(e)}")
        traceback.print_exc()
        return 0

def process_all_users_optimized():
    """
    Оптимизированная обработка всех пользователей
    """
    print("\n" + "="*80)
    print(f"🚀 НАЧАЛО ОБРАБОТКИ (SSL: {'Отключена' if DISABLE_SSL_VERIFY else 'Включена'})")
    print(f"📊 Загружаем данные за {HOURS_AGO} часов для каждого пользователя")
    print("="*80)
    
    all_users = get_all_users_from_ottai_cached()
    
    if not all_users:
        print("❌ Не удалось получить пользователей из Ottai")
        return
    
    # Отображаем мастеров
    master_statuses = display_available_masters(all_users)
    
    # Фильтруем настроенных пользователей
    configured_users = []
    for user in all_users:
        email = extract_clean_email(user['email']) or user['email']
        user_name = user.get('userName', '')
        user_id = user['fromUserId']
        ns_url, ns_secret = get_nightscout_config_by_email(email, user_name, user_id)
        if ns_url and ns_secret:
            configured_users.append({
                'email': email,
                'fromUserId': user_id,
                'userName': user_name,
            })
    
    print(f"\n[INFO] Настроено пользователей: {len(configured_users)}")
    
    if not configured_users:
        print("\n💡 ДОБАВЬТЕ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ:")
        print("   Для каждого пользователя нужно добавить две переменные:")
        print()

        for master in master_statuses:
            if not master['configured']:
                label = master['clean_email'] or master['email'] or master['user_name'] or str(master['user_id'])
                print(f"   Для пользователя '{label}':")
                # Ключ: email → userName → userId
                key = normalize_email_key(master['clean_email'] or master['email']) \
                      or master['user_name'] \
                      or str(master['user_id'])
                if key:
                    print(f"   NS_URL__{key}=https://ваш_nightscout.herokuapp.com")
                    print(f"   NS_SECRET__{key}=ваш_секрет")
                print()
        return
    
    total_successful = 0
    
    # Параллельная обработка пользователей
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = []
        for user_info in configured_users:
            future = executor.submit(process_user_wrapper, user_info)
            futures.append(future)
        
        for future in concurrent.futures.as_completed(futures):
            total_successful += future.result()
    
    _cleanup_old_cache()
    
    print("\n" + "="*80)
    print(f"📊 ИТОГ: Успешно обработано {total_successful} записей")
    print("="*80)

def _cleanup_old_cache():
    """Очистка устаревших кэшей"""
    current_time = time.time()
    keys_to_remove = []
    
    for key, (_, timestamp) in _connection_cache.items():
        if current_time - timestamp > 300:
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        del _connection_cache[key]
    
    if current_time - _user_cache['timestamp'] > 600:
        _user_cache['data'] = None
        _user_cache['timestamp'] = 0