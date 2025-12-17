import ssl
import urllib3
import warnings

# Глобально отключаем проверку SSL для решения проблемы с сертификатами
ssl._create_default_https_context = ssl._create_unverified_context
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore', message='Unverified HTTPS request')

from module import *
import schedule
import time
import datetime
import os
import sys

def print_banner():
    """Вывод заголовка программы"""
    banner = """
╔════════════════════════════════════════════════════════════════════╗
║               OTTAI TO NIGHTSCOUT SYNC MODULE v2.0                 ║
║               Многопользовательский режим с отладкой               ║
╚════════════════════════════════════════════════════════════════════╝
"""
    print(banner)

def print_system_info():
    """Вывод системной информации"""
    print("📋 СИСТЕМНАЯ ИНФОРМАЦИЯ:")
    print(f"   Время запуска: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Период загрузки данных: {HOURS_AGO} часов")
    print(f"   Базовая ссылка Ottai: {OTTAI_BASE_URL}")
    print(f"   SSL проверка: {'Отключена' if DISABLE_SSL_VERIFY else 'Включена'}")
    
    # Показываем найденные конфигурации Nightscout
    config_count = get_all_nightscout_configs_display()
    if config_count > 0:
        print(f"   Найдено конфигураций Nightscout: {config_count}")
    else:
        print(f"   Конфигурации Nightscout: не найдены")
    print()

def start_module():
    """Основная функция обработки данных"""
    os.system('cls' if os.name == 'nt' else 'clear')
    
    print_banner()
    print_system_info()
    
    # Обрабатываем данные для всех пользователей
    process_all_users_optimized()

def main():
    """Главная функция с планировщиком"""
    try:
        start_module()
    except KeyboardInterrupt:
        print("\n\n⏹️  Программа остановлена пользователем")
        return
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка при запуске: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Настройка периодического выполнения
    print("\n" + "="*80)
    print("⏰ ПЛАНИРОВЩИК АКТИВЕН")
    print("="*80)
    print("📅 Расписание: запуск каждую минуту")
    print("⏹️  Для остановки нажмите Ctrl+C")
    print("="*80 + "\n")
    
    schedule.every(1).minutes.do(start_module)
    
    # Основной цикл
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n👋 Программа остановлена. Всего доброго!")
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка в основном цикле: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()