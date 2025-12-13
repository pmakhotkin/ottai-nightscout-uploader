from module import *
import schedule
import time
import datetime
import os

def start_module():
    """Основная функция обработки данных"""
    os.system('cls' if os.name == 'nt' else 'clear')
    print(f"=== Запуск обработки: {datetime.datetime.now().strftime('%H:%M:%S')} ===")
    
    # Получаем данные из Ottai
    ottai_data = get_ottai_array_of_entries()
    
    if ottai_data is None:
        print("Не удалось получить данные от Ottai")
        return
    
    # Обрабатываем JSON данные
    process_json_data(ottai_data)
    
    print(f"=== Обработка завершена ===")

def main():
    """Главная функция с планировщиком"""
    print("Ottai to Nightscout Sync Module")
    print("===============================")
    
    # Запуск немедленной обработки
    try:
        start_module()
    except KeyboardInterrupt:
        print("\nПрограмма остановлена")
        return
    except Exception as e:
        print(f"Ошибка при запуске: {e}")
        return
    
    # Настройка периодического выполнения
    print("\nПланировщик: запуск каждую минуту")
    print("Для остановки нажмите Ctrl+C\n")
    
    schedule.every(1).minutes.do(start_module)
    
    # Основной цикл
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nПрограмма остановлена")
    except Exception as e:
        print(f"Критическая ошибка: {e}")

if __name__ == "__main__":
    main()