from module import *
import schedule
import time
import datetime
import os
import sys

def print_banner():
    """Вывод заголовка программы"""
    print("\n" + "="*60)
    print("OTTAI → NIGHTSCOUT SYNC v2.0 (ОПТИМИЗИРОВАННЫЙ)")
    print("="*60)

def print_system_info():
    """Вывод системной информации"""
    print("\n📋 СИСТЕМНАЯ ИНФОРМАЦИЯ:")
    print(f"   Время: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Загрузка данных: за {HOURS_AGO} часов")
    print(f"   Ottai URL: {OTTAI_BASE_URL}")
    print(f"   Макс. потоков: {MAX_WORKERS}")
    print(f"   Размер пачки: {BATCH_SIZE} записей")
    
    configs = get_all_nightscout_configs()
    print(f"   Nightscout конфигураций: {len(configs)}")

def start_module():
    """Основная функция обработки данных"""
    os.system('cls' if os.name == 'nt' else 'clear')
    
    print_banner()
    print_system_info()
    
    # Засекаем время выполнения
    start_time = time.time()
    
    # Обрабатываем данные для всех пользователей
    process_all_users_optimized()
    
    # Выводим время выполнения
    elapsed = time.time() - start_time
    print(f"⏱️  Время выполнения: {elapsed:.2f} секунд")

def main():
    """Главная функция с планировщиком"""
    try:
        start_module()
    except KeyboardInterrupt:
        print("\n\n⏹️  Программа остановлена пользователем")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print("\n" + "="*60)
    print("⏰ ПЛАНИРОВЩИК АКТИВЕН")
    print("="*60)
    print("📅 Запуск каждую минуту")
    print("⏹️  Ctrl+C для остановки")
    print("="*60)
    
    # Запускаем по расписанию
    schedule.every(1).minutes.do(start_module)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n👋 Программа остановлена")
    except Exception as e:
        print(f"\n\n❌ Ошибка в основном цикле: {e}")

if __name__ == "__main__":
    main()