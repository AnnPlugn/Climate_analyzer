"""
Скрипт для тестирования Weather Dask ETL API
Запуск: python test_api.py
"""

import requests
import time
import json

BASE_URL = "http://localhost:8000"

def print_response(response, title=""):
    """Красивый вывод ответа"""
    print(f"\n{'='*60}")
    if title:
        print(f"{title}")
    print(f"{'='*60}")
    print(f"Status Code: {response.status_code}")
    try:
        print(f"Response:\n{json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    except:
        print(f"Response: {response.text}")
    print(f"{'='*60}\n")

def test_ingest(start_date="2023-01-01", end_date="2023-01-31"):
    """Тест сбора данных"""
    print("🔄 Запуск Ingestion (сбор данных)...")
    url = f"{BASE_URL}/etl/ingest"
    params = {
        "start_date": start_date,
        "end_date": end_date
    }
    
    start_time = time.time()
    response = requests.post(url, params=params)
    duration = time.time() - start_time
    
    print_response(response, f"✅ Ingestion завершен за {duration:.2f} сек")
    return response.status_code == 200

def test_analyze():
    """Тест распределенной обработки"""
    print("🔄 Запуск Analysis (распределенная обработка)...")
    url = f"{BASE_URL}/etl/analyze"
    
    response = requests.get(url)
    print_response(response, "📊 Результаты анализа")
    return response.status_code == 200

def test_clean():
    """Тест очистки данных"""
    print("🔄 Очистка данных...")
    url = f"{BASE_URL}/etl/clean"
    
    response = requests.delete(url)
    print_response(response, "🗑️ Очистка завершена")
    return response.status_code == 200

def test_full_cycle():
    """Полный цикл тестирования"""
    print("\n" + "="*60)
    print("🚀 НАЧАЛО ПОЛНОГО ЦИКЛА ТЕСТИРОВАНИЯ")
    print("="*60)
    
    # Шаг 1: Очистка
    print("\n📌 Шаг 1: Очистка старых данных")
    test_clean()
    time.sleep(1)
    
    # Шаг 2: Сбор данных (быстрый тест - 1 месяц)
    print("\n📌 Шаг 2: Сбор данных за январь 2023")
    if not test_ingest("2023-01-01", "2023-01-31"):
        print("❌ Ошибка при сборе данных!")
        return
    
    time.sleep(2)  # Даем время на сохранение файлов
    
    # Шаг 3: Анализ
    print("\n📌 Шаг 3: Распределенная обработка данных")
    if not test_analyze():
        print("❌ Ошибка при анализе! Убедитесь, что данные были собраны.")
        return
    
    print("\n" + "="*60)
    print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
    print("="*60)
    print("\n💡 Совет: Откройте Dask Dashboard для визуализации:")
    print("   http://localhost:8787/status")
    print("\n💡 Swagger UI для интерактивного тестирования:")
    print("   http://localhost:8000/docs")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "ingest":
            start = sys.argv[2] if len(sys.argv) > 2 else "2023-01-01"
            end = sys.argv[3] if len(sys.argv) > 3 else "2023-01-31"
            test_ingest(start, end)
        elif command == "analyze":
            test_analyze()
        elif command == "clean":
            test_clean()
        else:
            print("Использование:")
            print("  python test_api.py          # Полный цикл")
            print("  python test_api.py ingest [start_date] [end_date]")
            print("  python test_api.py analyze")
            print("  python test_api.py clean")
    else:
        test_full_cycle()

