from setup import *
import requests
import json
import datetime
from datetime import timedelta
import traceback

# Явно импортируем необходимые переменные и функции из setup
from setup import HOURS_AGO, ns_unit_convert, ns_uploder, ns_url, ns_header
from setup import ottai_base_url, ottai_header_one_entries, init_ottai_headers

# ВАЖНО: импортируем сам модуль setup для доступа к глобальной переменной
import setup

def convert_mmoll_to_mgdl(x):
    """Конвертация ммоль/л в мг/дл"""
    try:
        return round(float(x) * ns_unit_convert)
    except (TypeError, ValueError) as e:
        print(f"Ошибка конвертации значения {x}: {e}")
        return 0

def convert_mgdl_to_mmoll(x):
    """Конвертация мг/дл в ммоль/л"""
    try:
        return round(float(x) / ns_unit_convert, 1)
    except (TypeError, ValueError) as e:
        print(f"Ошибка конвертации значения {x}: {e}")
        return 0.0

def get_last_entry_date():
    """Получение даты последней записи из Nightscout"""
    try:
        url = f"{ns_url}/api/v1/slice/entries/dateString/sgv/.*/.*?count=1"
        r = requests.get(url, headers=ns_header, timeout=10)
        
        print(f"Nightscout request: {r.status_code} {r.reason}")
        
        if r.status_code != 200:
            print(f"Ошибка Nightscout: {r.status_code}")
            return None
        
        data = r.json()
        
        if not data or not isinstance(data, list) or len(data) == 0:
            print("No data in Nightscout")
            return None
        
        last_entry = data[0]
        if 'date' not in last_entry:
            print("В записи нет поля 'date'")
            return None
            
        last_date = last_entry["date"]
        print(f"Last entry date: {last_date} (GMT {datetime.datetime.utcfromtimestamp(last_date/1000)})")
        return last_date
        
    except Exception as error:
        print(f"Error getting last entry date: {error}")
        return None

def get_ottai_one_entry():
    """Получение одной записи из Ottai (для тестирования)"""
    try:
        url = f"{ottai_base_url}/link/application/app/tagFromInviteLink/linkQueryList/v2"
        r = requests.post(url, headers=ottai_header_one_entries, timeout=30)
        
        print(f"Ottai Response Status: {r.status_code} {r.reason}")
        
        if r.status_code != 200:
            print(f"HTTP ошибка {r.status_code}: {r.text[:200]}")
            return None
            
        data = r.json()
        print(f"Структура ответа одной записи: {json.dumps(data, indent=2)[:500]}")
        return data
        
    except Exception as error:
        print(f"Ошибка получения записи Ottai: {error}")
        return None

def get_fromUserId():
    """Получение fromUserId из Ottai"""
    try:
        url = f"{ottai_base_url}/link/application/app/tagFromInviteLink/linkQueryList/v2"
        r = requests.post(url, headers=ottai_header_one_entries, timeout=30)
        
        print(f"Ottai UserId Response: {r.status_code} {r.reason}")
        
        if r.status_code != 200:
            print(f"Ошибка получения UserId: {r.status_code}")
            return None
            
        data = r.json()
        
        # Отладка структуры
        print(f"Структура ответа для UserId: {json.dumps(data, indent=2)[:1000]}")
        
        # Ищем fromUserId в различных местах
        fromUserId = None
        
        # Вариант 1: в корне ответа
        if 'fromUserId' in data:
            fromUserId = data['fromUserId']
            print(f"Найден fromUserId в корне: {fromUserId}")
        # Вариант 2: в data -> fromUserId
        elif 'data' in data and isinstance(data['data'], dict) and 'fromUserId' in data['data']:
            fromUserId = data['data']['fromUserId']
            print(f"Найден fromUserId в data: {fromUserId}")
        # Вариант 3: в data[0] -> fromUserId (самый вероятный вариант)
        elif 'data' in data and isinstance(data['data'], list) and len(data['data']) > 0:
            first_item = data['data'][0]
            if 'fromUserId' in first_item:
                fromUserId = first_item['fromUserId']
                print(f"Найден fromUserId в data[0]: {fromUserId}")
            elif 'id' in first_item:
                # Если fromUserId нет, но есть id, используем его
                fromUserId = first_item['id']
                print(f"Используем id как fromUserId: {fromUserId}")
        # Вариант 4: в user -> id
        elif 'user' in data and 'id' in data['user']:
            fromUserId = data['user']['id']
            print(f"Найден fromUserId в user.id: {fromUserId}")
        
        if fromUserId:
            return str(fromUserId)
        else:
            print("Не удалось найти fromUserId в ответе. Доступные ключи:")
            if isinstance(data, dict):
                print(list(data.keys()))
            return None
            
    except Exception as error:
        print(f"Ошибка получения fromUserId: {error}")
        traceback.print_exc()
        return None

def get_ottai_array_of_entries(lastDate=None):
    """
    Получение массива записей из Ottai
    
    Args:
        lastDate: Начальная дата в миллисекундах. Если None, используется HOURS_AGO часов назад
    """
    # Если lastDate не указан, используем значение по умолчанию
    if lastDate is None:
        lastDate = int(round((datetime.datetime.now() - timedelta(hours=HOURS_AGO)).timestamp() * 1000))
    
    # 1. Инициализируем заголовки через модуль setup
    if setup.ottai_header_array_entries is None:
        print("Инициализируем заголовки...")
        init_ottai_headers()
    
    # 2. Получаем заголовки напрямую из модуля setup
    headers = setup.ottai_header_array_entries
    
    # 3. Получаем fromUserId
    fromUserId = get_fromUserId()
    if fromUserId is None:
        print("Не удалось получить fromUserId. Завершаем запрос.")
        return None
    
    currentDate = int(round(datetime.datetime.now().timestamp() * 1000))
    
    # Формируем параметры запроса
    params = {
        'fromUserId': fromUserId,
        'isOpen': 0,  # Важный параметр из примера
        'startTime': lastDate,
        'endTime': currentDate
    }
    
    try:
        url = f"{ottai_base_url}/link/application/search/tag/queryMonitorBase"
        
        print(f"Заголовки запроса (без authorization):")
        safe_headers = {k: v for k, v in headers.items() if k != 'authorization'}
        print(json.dumps(safe_headers, indent=2))
        print(f"Параметры запроса: {params}")
        
        r = requests.get(url, 
                        headers=headers, 
                        params=params,
                        timeout=30)
        
        print(f"Ottai get entries Response Status: {r.status_code} {r.reason}")
        print(f"Content-Type: {r.headers.get('Content-Type', 'не указан')}")
        print(f"URL: {r.url}")
        
        # Выводим первые 1000 символов ответа для отладки
        print(f"Ответ сервера (первые 1000 символов): {r.text[:1000]}")
        
        if r.status_code != 200:
            print(f"HTTP ошибка {r.status_code}: {r.reason}")
            print(f"Тело ответа: {r.text[:500]}")
            return None
        
        # Проверяем Content-Type
        content_type = r.headers.get('Content-Type', '').lower()
        if 'application/json' not in content_type:
            print(f"Предупреждение: Content-Type не JSON: {content_type}")
            print(f"Полученный ответ: {r.text[:500]}")
            return None
            
        data = r.json()
        
        # Выводим структуру ответа для отладки
        print(f"Структура ответа: {json.dumps(data, indent=2)[:2000]}")
        
        return data
        
    except requests.JSONDecodeError as e:
        print(f"Ошибка декодирования JSON: {e}")
        print(f"Сырой ответ: {r.text[:1000] if 'r' in locals() else 'Ответ недоступен'}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка в get_ottai_array_of_entries: {e}")
        traceback.print_exc()
        return None

def process_json_data(data):
    """Обработка JSON данных и загрузка в Nightscout"""
    if data is None:
        print("Нет данных для обработки")
        return
    
    try:
        # Выводим полную структуру данных для отладки
        print(f"Полная структура данных для обработки: {json.dumps(data, indent=2)[:3000]}")
        
        # Проверяем структуру данных
        if not isinstance(data, dict):
            print(f"Некорректный тип данных: {type(data)}. Ожидался словарь.")
            print(f"Данные: {data}")
            return
        
        # Ищем данные в различных возможных структурах
        curve_list = None
        
        # Вариант 1: data -> data -> curveList
        if 'data' in data and isinstance(data['data'], dict) and 'curveList' in data['data']:
            curve_list = data['data']['curveList']
            print(f"Найдена структура data->data->curveList: {len(curve_list)} записей")
        
        # Вариант 2: data -> curveList
        elif 'curveList' in data and isinstance(data['curveList'], list):
            curve_list = data['curveList']
            print(f"Найдена структура data->curveList: {len(curve_list)} записей")
        
        # Вариант 3: данные прямо в корне (список)
        elif isinstance(data, list):
            curve_list = data
            print(f"Данные в корневом списке: {len(curve_list)} записей")
        
        else:
            print(f"Неизвестная структура данных. Ключи: {list(data.keys())}")
            return
        
        if not curve_list or not isinstance(curve_list, list):
            print(f"curveList не является списком или пуст. Тип: {type(curve_list)}")
            return
            
        print(f"Найдено {len(curve_list)} записей для обработки")
        
        successful_uploads = 0
        for i, item in enumerate(curve_list):
            try:
                if not isinstance(item, dict):
                    print(f"Запись {i} не является словарем: {type(item)}")
                    continue
                
                # Проверяем обязательные поля
                required_fields = ['adjustGlucose', 'monitorTime']
                missing_fields = [field for field in required_fields if field not in item]
                
                if missing_fields:
                    print(f"Пропуск записи {i}: отсутствуют поля {missing_fields}")
                    print(f"Доступные поля: {list(item.keys())}")
                    continue
                
                # Конвертируем значения
                try:
                    glucose = float(item['adjustGlucose'])
                    timestamp = int(item['monitorTime'])
                except (ValueError, TypeError) as e:
                    print(f"Ошибка преобразования значений в записи {i}: {e}")
                    continue
                
                entry_dict = {
                    "type": "sgv",
                    "sgv": convert_mmoll_to_mgdl(glucose),
                    "direction": "FortyFiveUp",  # TODO: Рассчитать реальное направление
                    "device": ns_uploder,
                    "date": timestamp,
                    "dateString": datetime.datetime.utcfromtimestamp(timestamp/1000).isoformat(timespec='milliseconds') + "Z"
                }
                
                print(f"Обработка записи {i}: {glucose} ммоль/л на {datetime.datetime.fromtimestamp(timestamp/1000)}")
                
                # Загружаем запись
                if upload_entry(entry_dict):
                    successful_uploads += 1
                    
            except Exception as item_error:
                print(f"Ошибка обработки записи {i}: {item_error}")
                traceback.print_exc()
                continue
        
        print(f"Успешно загружено {successful_uploads} из {len(curve_list)} записей")
        
    except Exception as error:
        print(f"Error reading glucose data: {error}")
        traceback.print_exc()

def upload_entry(entry_dict):
    """Загрузка одной записи в Nightscout"""
    try:
        r = requests.post(f"{ns_url}/entries", 
                         headers=ns_header, 
                         json=entry_dict,
                         timeout=10)
        
        if r.status_code == 200:
            time_str = datetime.datetime.fromtimestamp(entry_dict['date']/1000).strftime('%Y-%m-%d %H:%M:%S')
            print(f"✓ Успешно загружена запись на {time_str}: {entry_dict['sgv']} мг/дл")
            return True
        else:
            print(f"✗ Ошибка загрузки записи: {r.status_code} {r.reason}")
            if r.text:
                print(f"   Ответ сервера: {r.text[:200]}")
            return False
            
    except Exception as error:
        print(f"✗ Ошибка при загрузке записи в Nightscout: {error}")
        return False

def get_query_entry_date(query_date, header):
    """Получение записи по конкретной дате"""
    try:
        url = f"{ns_url}/slice/entries/dateString/sgv/{query_date}.*"
        r = requests.get(url, headers=header, timeout=10)
        
        print(f"{datetime.datetime.now()} Nightscout request: {r.status_code} {r.reason}")
        
        if r.status_code != 200:
            print(f"Ошибка запроса: {r.status_code}")
            return None
            
        data = r.json()
        
        if not data:
            print(f"Нет данных за дату {query_date}")
            return None
            
        print(f"Last entry date {data[0]['date']} GMT {datetime.datetime.utcfromtimestamp(data[0]['date']/1000)}")
        return data[0]['date']
        
    except Exception as error:
        print(f"Error getting query entry date: {error}")
        return None