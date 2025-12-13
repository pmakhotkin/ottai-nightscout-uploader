from setup import *
import requests
import json
import datetime
from datetime import timedelta
import traceback

# Импортируем необходимые переменные и функции из setup
from setup import HOURS_AGO, ns_unit_convert, ns_uploder, ns_url, ns_header
from setup import ottai_base_url, ottai_header_one_entries, init_ottai_headers

# Импортируем сам модуль setup для доступа к глобальной переменной
import setup

def convert_mmoll_to_mgdl(x):
    """Конвертация ммоль/л в мг/дл"""
    try:
        return round(float(x) * ns_unit_convert)
    except (TypeError, ValueError):
        return 0

def convert_mgdl_to_mmoll(x):
    """Конвертация мг/дл в ммоль/л"""
    try:
        return round(float(x) / ns_unit_convert, 1)
    except (TypeError, ValueError):
        return 0.0

def get_last_entry_date():
    """Получение даты последней записи из Nightscout"""
    try:
        url = f"{ns_url}/api/v1/slice/entries/dateString/sgv/.*/.*?count=1"
        r = requests.get(url, headers=ns_header, timeout=10)
        
        if r.status_code != 200:
            return None
        
        data = r.json()
        
        if not data or not isinstance(data, list) or len(data) == 0:
            return None
        
        last_entry = data[0]
        if 'date' not in last_entry:
            return None
            
        return last_entry["date"]
        
    except Exception:
        return None

def get_fromUserId():
    """Получение fromUserId из Ottai"""
    try:
        url = f"{ottai_base_url}/link/application/app/tagFromInviteLink/linkQueryList/v2"
        r = requests.post(url, headers=ottai_header_one_entries, timeout=30)
        
        if r.status_code != 200:
            return None
            
        data = r.json()
        
        # Ищем fromUserId в различных местах
        fromUserId = None
        
        if 'fromUserId' in data:
            fromUserId = data['fromUserId']
        elif 'data' in data and isinstance(data['data'], dict) and 'fromUserId' in data['data']:
            fromUserId = data['data']['fromUserId']
        elif 'data' in data and isinstance(data['data'], list) and len(data['data']) > 0:
            first_item = data['data'][0]
            if 'fromUserId' in first_item:
                fromUserId = first_item['fromUserId']
            elif 'id' in first_item:
                fromUserId = first_item['id']
        elif 'user' in data and 'id' in data['user']:
            fromUserId = data['user']['id']
        
        return str(fromUserId) if fromUserId else None
            
    except Exception:
        return None

def get_ottai_array_of_entries():
    """
    Получение массива записей из Ottai с правильной дельтой
    """
    # 1. Определяем начальное время (дельта)
    last_ns_date = get_last_entry_date()
    
    if last_ns_date:
        # Если есть записи в Nightscout, начинаем с последней + 1 мс
        start_time = last_ns_date + 1
        print(f"Продолжаем с последней записи Nightscout")
    else:
        # Если в Nightscout нет данных, загружаем данные за HOURS_AGO часов
        start_time = int(round((datetime.datetime.now() - timedelta(hours=HOURS_AGO)).timestamp() * 1000))
        print(f"Загружаем данные за последние {HOURS_AGO} часов")
    
    # 2. Инициализируем заголовки
    if setup.ottai_header_array_entries is None:
        init_ottai_headers()
    
    headers = setup.ottai_header_array_entries
    
    # 3. Получаем fromUserId
    fromUserId = get_fromUserId()
    if fromUserId is None:
        print("Не удалось получить fromUserId")
        return None
    
    current_time = int(round(datetime.datetime.now().timestamp() * 1000))
    
    # Если начальное время больше или равно текущему, нет новых данных
    if start_time >= current_time:
        print("Нет новых данных для загрузки")
        return None
    
    # Формируем параметры запроса
    params = {
        'fromUserId': fromUserId,
        'isOpen': 0,
        'startTime': start_time,
        'endTime': current_time
    }
    
    try:
        url = f"{ottai_base_url}/link/application/search/tag/queryMonitorBase"
        
        print(f"Запрос данных за период: {datetime.datetime.fromtimestamp(start_time/1000).strftime('%H:%M')} - {datetime.datetime.fromtimestamp(current_time/1000).strftime('%H:%M')}")
        
        r = requests.get(url, 
                        headers=headers, 
                        params=params,
                        timeout=30)
        
        if r.status_code != 200:
            print(f"Ошибка запроса: {r.status_code}")
            return None
        
        # Проверяем Content-Type
        content_type = r.headers.get('Content-Type', '').lower()
        if 'application/json' not in content_type:
            print(f"Ошибка: получен не JSON ответ")
            return None
            
        data = r.json()
        
        # Проверяем код ответа
        if 'code' in data and data['code'] != "OK":
            print(f"Ошибка от Ottai: {data.get('msg', 'Unknown error')}")
            return None
        
        return data
        
    except requests.JSONDecodeError:
        print("Ошибка декодирования JSON ответа")
        return None
    except Exception as e:
        print(f"Ошибка при получении данных: {str(e)}")
        return None

def process_json_data(data):
    """Обработка JSON данных и загрузка в Nightscout"""
    if data is None:
        print("Нет данных для обработки")
        return
    
    try:
        # Ищем данные в структуре
        curve_list = None
        
        if 'data' in data and isinstance(data['data'], dict) and 'curveList' in data['data']:
            curve_list = data['data']['curveList']
        elif 'curveList' in data and isinstance(data['curveList'], list):
            curve_list = data['curveList']
        else:
            print("Неизвестная структура данных")
            return
        
        if not curve_list or not isinstance(curve_list, list):
            print("Нет данных для загрузки")
            return
            
        successful_uploads = 0
        total_records = len(curve_list)
        
        for item in curve_list:
            try:
                if not isinstance(item, dict):
                    continue
                
                # Проверяем обязательные поля
                if 'adjustGlucose' not in item or 'monitorTime' not in item:
                    continue
                
                # Конвертируем значения
                try:
                    glucose = float(item['adjustGlucose'])
                    timestamp = int(item['monitorTime'])
                except (ValueError, TypeError):
                    continue
                
                entry_dict = {
                    "type": "sgv",
                    "sgv": convert_mmoll_to_mgdl(glucose),
                    "direction": "FortyFiveUp",
                    "device": ns_uploder,
                    "date": timestamp,
                    "dateString": datetime.datetime.utcfromtimestamp(timestamp/1000).isoformat(timespec='milliseconds') + "Z"
                }
                
                # Загружаем запись
                if upload_entry(entry_dict):
                    successful_uploads += 1
                    
            except Exception:
                continue
        
        if successful_uploads > 0:
            print(f"Загружено {successful_uploads} новых записей")
        
    except Exception as error:
        print(f"Ошибка обработки данных: {error}")

def upload_entry(entry_dict):
    """Загрузка одной записи в Nightscout"""
    try:
        r = requests.post(f"{ns_url}/entries", 
                         headers=ns_header, 
                         json=entry_dict,
                         timeout=10)
        
        if r.status_code == 200:
            return True
        else:
            return False
            
    except Exception:
        return False