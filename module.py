from setup import *
import requests
import json
import datetime
from datetime import timedelta
import traceback
import re

# Импортируем необходимые компоненты из setup
from setup import (
    HOURS_AGO, ns_unit_convert, 
    get_ottai_headers_for_user,
    ottai_base_url, ottai_header_one_entries,
    get_nightscout_config_by_email, extract_clean_email, normalize_email_key,
    get_all_nightscout_configs, nightscout_configs,
    get_hash_SHA1
)

def convert_mmoll_to_mgdl(x):
    """Конвертация ммоль/л в мг/дл"""
    try:
        return round(float(x) * ns_unit_convert)
    except (TypeError, ValueError):
        return 0

def get_all_users_from_ottai():
    """
    Получение списка всех пользователей из Ottai
    """
    try:
        url = f"{ottai_base_url}/link/application/app/tagFromInviteLink/linkQueryList/v2"
        print(f"\n[DEBUG] === ЗАПРОС СПИСКА ПОЛЬЗОВАТЕЛЕЙ ===")
        print(f"[DEBUG] Метод: POST")
        print(f"[DEBUG] URL: {url}")
        
        r = requests.post(url, headers=ottai_header_one_entries, timeout=30)
        
        print(f"[DEBUG] Ответ: {r.status_code} {r.reason}")
        
        if r.status_code != 200:
            print(f"[ERROR] Ошибка запроса пользователей: {r.status_code}")
            if r.text:
                print(f"[DEBUG] Тело ответа: {r.text[:500]}")
            return []
            
        data = r.json()
        
        if 'data' in data and isinstance(data['data'], list):
            users = []
            for user_item in data['data']:
                email = None
                possible_email_fields = ['fromUserEmail', 'remark', 'email', 'userEmail']
                
                for field in possible_email_fields:
                    if field in user_item and user_item[field]:
                        email = user_item[field]
                        email = email.strip()
                        break
                
                user_id = user_item.get('fromUserId') or user_item.get('id')
                
                if email and user_id:
                    users.append({
                        'email': email,
                        'fromUserId': user_id,
                        'raw_data': user_item
                    })
                    print(f"[DEBUG] Найден пользователь: {email} (ID: {user_id})")
            
            print(f"[DEBUG] Всего пользователей: {len(users)}")
            return users
        else:
            print("[ERROR] Неожиданная структура ответа")
            return []
            
    except Exception as e:
        print(f"[ERROR] Ошибка при получении пользователей: {str(e)}")
        traceback.print_exc()
        return []

def create_user_config(user_email, from_user_id):
    """
    Создание конфигурации пользователя
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
    user_config['ottai_headers'] = get_ottai_headers_for_user()
    
    return user_config

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
        clean_email = extract_clean_email(email)
        
        ns_url, ns_secret = get_nightscout_config_by_email(clean_email or email)
        status = "✅ НАСТРОЕН" if ns_url and ns_secret else "❌ НЕ НАСТРОЕН"
        
        config_key = "—"
        if ns_url and ns_secret:
            config_key = normalize_email_key(clean_email or email) or "unknown"
        
        master_statuses.append({
            'index': idx,
            'email': email,
            'clean_email': clean_email,
            'user_id': user_id,
            'configured': bool(ns_url and ns_secret),
            'config_key': config_key
        })
        
        print(f"{idx:2d}. {email}")
        print(f"    ID: {user_id}")
        print(f"    Статус: {status}")
        if ns_url and ns_secret:
            print(f"    Конфиг: {config_key}")
            print(f"    Nightscout URL: {ns_url[:50]}...")
        print()
    
    return master_statuses

def check_nightscout_connection(user_config):
    """
    Проверка соединения с Nightscout
    """
    try:
        base_url = user_config['ns_url']
        if not base_url.endswith('/api/v1'):
            base_url = f"{base_url}/api/v1"
        
        url = f"{base_url}/status"
        print(f"[DEBUG] Проверка соединения с Nightscout: GET {url}")
        
        r = requests.get(url, headers=user_config['ns_header'], timeout=10)
        
        print(f"[DEBUG] Статус Nightscout: {r.status_code}")
        if r.status_code == 200:
            print(f"[DEBUG] ✅ Nightscout доступен")
            return True
        else:
            print(f"[DEBUG] ❌ Nightscout ошибка: {r.status_code}")
            if r.text:
                print(f"[DEBUG] Ответ Nightscout: {r.text[:200]}")
            return False
            
    except Exception as e:
        print(f"[DEBUG] ❌ Ошибка соединения с Nightscout: {str(e)}")
        return False

def get_last_entry_date(user_config):
    """
    Получение даты последней записи из Nightscout (используем стандартный API)
    """
    try:
        base_url = user_config['ns_url']
        if not base_url.endswith('/api/v1'):
            base_url = f"{base_url}/api/v1"
        
        # Пробуем несколько вариантов стандартных endpoints Nightscout
        endpoints = [
            "/entries?count=1",
            "/entries.json?count=1",
            "/entries/sgv.json?count=1",
            "/entries?find[type]=sgv&count=1"
        ]
        
        for endpoint in endpoints:
            url = f"{base_url}{endpoint}"
            print(f"[DEBUG] Запрос последней записи: GET {url}")
            
            try:
                r = requests.get(url, headers=user_config['ns_header'], timeout=10)
                
                if r.status_code == 200:
                    data = r.json()
                    
                    if data and isinstance(data, list) and len(data) > 0:
                        last_entry = data[0]
                        if 'date' in last_entry:
                            last_date = last_entry['date']
                            date_str = datetime.datetime.fromtimestamp(last_date/1000).strftime('%Y-%m-%d %H:%M:%S')
                            print(f"[DEBUG] ✅ Последняя запись в Nightscout: {date_str}")
                            return last_date
                    else:
                        print(f"[DEBUG] Nightscout пуст")
                        return None
                else:
                    print(f"[DEBUG] Endpoint {endpoint} вернул статус: {r.status_code}")
                    
            except Exception as e:
                print(f"[DEBUG] Ошибка при запросе {endpoint}: {str(e)}")
                continue
        
        print(f"[DEBUG] ❌ Не удалось получить последнюю запись из Nightscout")
        return None
        
    except Exception as e:
        print(f"[DEBUG] Ошибка при получении последней записи: {str(e)}")
        traceback.print_exc()
        return None

def process_user_data(user_config):
    """
    Обработка данных для конкретного пользователя
    """
    print(f"\n[USER] === ОБРАБОТКА ПОЛЬЗОВАТЕЛЯ: {user_config['email']} ===")
    
    # 1. Проверяем соединение с Nightscout
    if not check_nightscout_connection(user_config):
        print(f"[ERROR] Nightscout недоступен для {user_config['email']}")
        return
    
    # 2. Получаем последнюю запись из Nightscout
    last_ns_date = get_last_entry_date(user_config)
    
    if last_ns_date:
        start_time = last_ns_date + 1
        start_str = datetime.datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[DEBUG] Продолжаем с: {start_str}")
    else:
        start_time = int(round((datetime.datetime.now() - timedelta(hours=HOURS_AGO)).timestamp() * 1000))
        start_str = datetime.datetime.fromtimestamp(start_time/1000).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[DEBUG] Загружаем данные за {HOURS_AGO} часов, начиная с: {start_str}")
    
    current_time = int(round(datetime.datetime.now().timestamp() * 1000))
    
    if start_time >= current_time:
        print(f"[INFO] Нет новых данных")
        return
    
    # 3. Формируем запрос к Ottai
    params = {
        'fromUserId': user_config['from_user_id'],
        'isOpen': 0,
        'startTime': start_time,
        'endTime': current_time
    }
    
    try:
        url = f"{ottai_base_url}/link/application/search/tag/queryMonitorBase"
        
        print(f"\n[DEBUG] === ЗАПРОС ДАННЫХ OTTAI ===")
        print(f"[DEBUG] Метод: GET")
        print(f"[DEBUG] URL: {url}")
        print(f"[DEBUG] Параметры: {params}")
        
        r = requests.get(url, 
                        headers=user_config['ottai_headers'], 
                        params=params,
                        timeout=30)
        
        print(f"[DEBUG] Ответ Ottai: {r.status_code} {r.reason}")
        
        if r.status_code != 200:
            print(f"[ERROR] Ошибка запроса Ottai: {r.status_code}")
            if r.text:
                print(f"[DEBUG] Тело ответа: {r.text[:500]}")
            return
        
        data = r.json()
        print(f"[DEBUG] Успешно получены данные от Ottai")
        
        # Выводим структуру ответа для отладки
        print(f"[DEBUG] Структура ответа: {json.dumps(data, indent=2)[:1000]}...")
        
        # 4. Обрабатываем данные
        successful_uploads = process_user_json_data(user_config, data)
        
        if successful_uploads > 0:
            print(f"[SUCCESS] ✅ Загружено {successful_uploads} записей в Nightscout")
        else:
            print(f"[INFO] ℹ️  Нет новых записей для загрузки")
        
    except Exception as e:
        print(f"[ERROR] Ошибка при обработке пользователя: {str(e)}")
        traceback.print_exc()

def process_user_json_data(user_config, data):
    """
    Обработка JSON данных и загрузка в Nightscout
    """
    if not data:
        return 0
    
    try:
        curve_list = None
        
        if 'data' in data and isinstance(data['data'], dict) and 'curveList' in data['data']:
            curve_list = data['data']['curveList']
        elif 'curveList' in data and isinstance(data['curveList'], list):
            curve_list = data['curveList']
        else:
            print(f"[DEBUG] Неизвестная структура данных")
            return 0
        
        if not curve_list:
            print(f"[DEBUG] Нет данных для обработки")
            return 0
        
        print(f"[DEBUG] Найдено {len(curve_list)} записей для обработки")
        
        successful_uploads = 0
        
        for i, item in enumerate(curve_list[:10]):  # Ограничим вывод первых 10 записей
            try:
                if 'adjustGlucose' not in item or 'monitorTime' not in item:
                    continue
                
                glucose = float(item['adjustGlucose'])
                timestamp = int(item['monitorTime'])
                time_str = datetime.datetime.fromtimestamp(timestamp/1000).strftime('%H:%M:%S')
                print(f"[DEBUG] Запись {i}: {glucose} ммоль/л в {time_str}")
                
                entry_dict = {
                    "type": "sgv",
                    "sgv": convert_mmoll_to_mgdl(glucose),
                    "direction": "Flat",  # По умолчанию
                    "device": user_config['ns_uploder'],
                    "date": timestamp,
                    "dateString": datetime.datetime.utcfromtimestamp(timestamp/1000).isoformat(timespec='milliseconds') + "Z"
                }
                
                # Пытаемся определить направление тренда
                if 'slope' in item or 'trend' in item:
                    trend_value = item.get('trend') or item.get('slope')
                    if trend_value:
                        trend_map = {
                            'rising': 'DoubleUp',
                            'falling': 'DoubleDown',
                            'stable': 'Flat'
                        }
                        entry_dict['direction'] = trend_map.get(trend_value, 'Flat')
                
                # Загружаем запись в Nightscout
                if upload_entry_to_nightscout(user_config, entry_dict):
                    successful_uploads += 1
                    
            except Exception as e:
                print(f"[DEBUG] Ошибка обработки записи {i}: {str(e)}")
                continue
        
        if len(curve_list) > 10:
            print(f"[DEBUG] ... и еще {len(curve_list) - 10} записей")
        
        return successful_uploads
        
    except Exception as error:
        print(f"[ERROR] Ошибка обработки JSON данных: {str(error)}")
        traceback.print_exc()
        return 0

def upload_entry_to_nightscout(user_config, entry_dict):
    """
    Загрузка одной записи в Nightscout
    """
    try:
        base_url = user_config['ns_url']
        if not base_url.endswith('/api/v1'):
            base_url = f"{base_url}/api/v1"
        
        url = f"{base_url}/entries"
        
        print(f"[DEBUG] Отправка в Nightscout: POST {url}")
        print(f"[DEBUG] Данные записи: {json.dumps(entry_dict, indent=2)}")
        
        r = requests.post(url, 
                         headers=user_config['ns_header'], 
                         json=entry_dict,
                         timeout=10)
        
        print(f"[DEBUG] Ответ Nightscout: {r.status_code} {r.reason}")
        
        if r.status_code == 200:
            time_str = datetime.datetime.fromtimestamp(entry_dict['date']/1000).strftime('%H:%M:%S')
            print(f"[DEBUG] ✅ Запись {time_str} успешно загружена")
            return True
        else:
            print(f"[DEBUG] ❌ Ошибка загрузки: {r.status_code}")
            if r.text:
                print(f"[DEBUG] Ответ Nightscout: {r.text[:200]}")
            return False
            
    except Exception as e:
        print(f"[ERROR] Ошибка при загрузке в Nightscout: {str(e)}")
        return False

def process_all_users():
    """
    Основная функция обработки данных
    """
    print("\n" + "="*80)
    print("=== НАЧАЛО ОБРАБОТКИ ===")
    print("="*80)
    
    # Получаем всех пользователей
    all_users = get_all_users_from_ottai()
    
    if not all_users:
        print("❌ Не удалось получить пользователей из Ottai")
        return
    
    # Отображаем мастеров
    master_statuses = display_available_masters(all_users)
    
    # Фильтруем настроенных пользователей
    configured_users = []
    for user in all_users:
        email = extract_clean_email(user['email']) or user['email']
        ns_url, ns_secret = get_nightscout_config_by_email(email)
        if ns_url and ns_secret:
            user_config = create_user_config(email, user['fromUserId'])
            if user_config:
                configured_users.append(user_config)
    
    print(f"\n[INFO] Настроено пользователей: {len(configured_users)}")
    
    if not configured_users:
        print("\n💡 ДОБАВЬТЕ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ:")
        print("   Для каждого пользователя нужно добавить две переменные:")
        print()
        
        for master in master_statuses:
            if not master['configured']:
                print(f"   Для пользователя '{master['email']}':")
                normalized_key = normalize_email_key(master['clean_email'] or master['email'])
                if normalized_key:
                    print(f"   NS_URL__{normalized_key}=https://ваш_nightscout.herokuapp.com")
                    print(f"   NS_SECRET__{normalized_key}=ваш_секрет")
                print()
        return
    
    # Обрабатываем каждого пользователя
    for user_config in configured_users:
        process_user_data(user_config)
    
    print("\n" + "="*80)
    print("=== ОБРАБОТКА ЗАВЕРШЕНА ===")
    print("="*80)