import time
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import text
from app.database import init_db, save_dataframe_to_db, engine, get_session
from app.main import CITIES
import requests

def wait_for_database(max_attempts=30, delay=2):
    """Ожидание готовности базы данных"""
    print("Ожидание готовности базы данных...")
    for attempt in range(max_attempts):
        try:
            init_db()
            session = get_session()
            session.execute(text("SELECT 1"))
            session.close()
            print("✅ База данных готова к работе")
            return True
        except Exception as e:
            print(f"⏳ Попытка {attempt + 1}/{max_attempts}: База данных еще не готова - {str(e)}")
            time.sleep(delay)
    print("❌ Не удалось подключиться к базе данных после максимального количества попыток")
    return False

def load_data_for_period(start_date, end_date, cities_list):
    """Загрузка данных для указанного периода и списка городов"""
    print(f"📥 Начало загрузки данных с {start_date} по {end_date} для {len(cities_list)} городов")
    
    for city_name in cities_list:
        if city_name not in CITIES:
            print(f"⚠️ Город {city_name} не найден в координатах")
            continue
            
        coords = CITIES[city_name]
        url = "https://archive-api.open-meteo.com/v1/archive"
        
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": "temperature_2m,apparent_temperature,dewpoint_2m,relative_humidity_2m,precipitation,rain,showers,snowfall,weather_code,wind_speed_10m,wind_gusts_10m,wind_direction_10m,surface_pressure,cloud_cover,shortwave_radiation,uv_index,sunshine_duration,is_day",
            "timezone": "auto"
        }
        
        print(f"🌍 Запрос данных для {city_name}...")
        try:
            # Добавляем повторные попытки для обработки временных сбоев сети
            for retry in range(3):
                try:
                    response = requests.get(url, params=params, timeout=60)
                    if response.status_code == 200:
                        break
                    print(f"⚠️ Попытка {retry + 1} не удалась для {city_name}, код: {response.status_code}")
                    time.sleep(5 * (retry + 1))
                except requests.exceptions.RequestException as e:
                    print(f"⚠️ Ошибка сети при запросе для {city_name}: {str(e)}")
                    time.sleep(10 * (retry + 1))
            else:
                print(f"❌ Все попытки запроса для {city_name} не удалась")
                continue
            
            data = response.json()
            
            # Проверка наличия данных
            if "hourly" not in data or not data["hourly"]:
                print(f"❌ Нет почасовых данных для {city_name}")
                continue
                
            # Создание DataFrame
            hourly_data = data["hourly"]
            time_data = hourly_data.get("time", [])
            
            if not time_data:
                print(f"❌ Пустые временные данные для {city_name}")
                continue
            
            # Создаем DataFrame только с существующими колонками
            data_dict = {
                "time": time_data,
                "city": [city_name] * len(time_data),
                "data_source": ["open-meteo"] * len(time_data),
                "last_updated": [datetime.utcnow()] * len(time_data)
            }
            
            # Добавляем только те параметры, которые есть в ответе
            for param in ["temperature_2m", "apparent_temperature", "dewpoint_2m", 
                         "relative_humidity_2m", "precipitation", "rain", "showers", 
                         "snowfall", "weather_code", "wind_speed_10m", "wind_gusts_10m", 
                         "wind_direction_10m", "surface_pressure", "cloud_cover", 
                         "shortwave_radiation", "uv_index", "sunshine_duration", "is_day"]:
                param_name = param.replace("_2m", "").replace("_10m", "")
                if param in hourly_data and hourly_data[param]:
                    data_dict[param_name] = hourly_data[param]
            
            df = pd.DataFrame(data_dict)
            
            # Сохранение в БД
            print(f"💾 Сохранение {len(df)} записей для {city_name} в базу данных...")
            save_dataframe_to_db(df, "weather_data")
            
            print(f"✅ Успешно загружены данные для {city_name}")
            
        except Exception as e:
            print(f"❌ Ошибка при загрузке данных для {city_name}: {str(e)}")

def fill_database():
    """Основная функция заполнения базы данных"""
    print("🚀 Запуск автоматического заполнения базы данных...")
    
    # Ждем готовности БД
    if not wait_for_database():
        print("❌ Невозможно продолжить: база данных недоступна")
        return False
    
    try:
        # 1. Глубокие данные за 2023 год для 10 ключевых городов
        key_cities = [
            "London", "Berlin", "Paris", "Moscow", 
            "New York", "Tokyo", "Sydney", 
            "Rio de Janeiro", "Cairo", "Singapore"
        ]
        print("📊 Загрузка подробных данных за 2023 год для ключевых городов...")
        load_data_for_period("2023-01-01", "2023-12-31", key_cities)
        
        # 2. Последние 30 дней для всех городов
        today = datetime.now().strftime("%Y-%m-%d")
        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        all_cities = list(CITIES.keys())
        print("🔄 Загрузка последних данных для всех городов...")
        load_data_for_period(thirty_days_ago, today, all_cities)
        
        print("🎉 Заполнение базы данных завершено успешно!")
        return True
        
    except Exception as e:
        print(f"❌ Критическая ошибка при заполнении базы данных: {str(e)}")
        return False

if __name__ == "__main__":
    fill_database()
