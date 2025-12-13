from module import *
import schedule
import time
import datetime
import os

def start_module():
    """Основная функция обработки данных"""
    # Очистка экрана
    os.system('cls' if os.name == 'nt' else 'clear')
    print("start of processing...")
    print("-" * 50)
    
    # 1. Получаем данные из Ottai
    try:
        ottai_data = get_ottai_array_of_entries()
        
        if ottai_data is None:
            print("Не удалось получить данные от Ottai. Завершаем.")
            return
        
        # Отладочная информация
        print(f"Тип полученных данных: {type(ottai_data)}")
        if isinstance(ottai_data, dict):
            print(f"Ключи в данных: {list(ottai_data.keys())}")
        elif isinstance(ottai_data, list):
            print(f"Количество элементов в массиве: {len(ottai_data)}")
        
        # 2. Обрабатываем JSON данные
        try:  
            process_json_data(ottai_data)
            print("✓ Entries loaded in Nightscout")
        except Exception as error:
            print(f"✗ Error reading direction: {error}")
            import traceback
            traceback.print_exc()
            
    except Exception as error:
        print(f"✗ Error requesting from ottai: {error}")
        import traceback
        traceback.print_exc()
        return
    
    print("-" * 50)
    print(f"Processing completed at: {datetime.datetime.now()}")
    print()

def get_last_entry_date_from_nightscout():
    """Получение даты последней записи из Nightscout"""
    try:
        ns_last_date = get_last_entry_date()
        if ns_last_date == "":
            ns_last_date = 0
        print(f"Last Nightscout entry date: {ns_last_date}")
        return ns_last_date
    except Exception as error:
        print(f"Warning: Error requesting from Nightscout: {error}")
        return 0

def main():
    """Главная функция с планировщиком"""
    print("=" * 60)
    print("Ottai to Nightscout Sync Module")
    print("=" * 60)
    
    # Запуск немедленной обработки
    try:
        start_module()
    except KeyboardInterrupt:
        print("\nПрограмма остановлена пользователем")
        return
    
    # Настройка периодического выполнения
    print("Setting up scheduler: running every 1 minute")
    print("Press Ctrl+C to stop\n")
    
    schedule.every(1).minutes.do(start_module)
    
    # Основной цикл
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nПрограмма остановлена. Всего доброго!")
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()