"""
Prefect Flow для оркестрации ETL процесса
"""

import os
from prefect import flow, task
import requests
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
from datetime import datetime
from app.database import init_db, save_dataframe_to_db, clear_table

# Настройки
DASK_SCHEDULER = os.getenv("DASK_SCHEDULER_ADDRESS", "127.0.0.1:8786")
DATA_DIR = "/data"

# Координаты городов
CITIES = {
    "London": {"lat": 51.50, "lon": -0.12},
    "Berlin": {"lat": 52.52, "lon": 13.41},
    "Madrid": {"lat": 40.41, "lon": -3.70},
    "Moscow": {"lat": 55.75, "lon": 37.61},
    "Paris":  {"lat": 48.85, "lon": 2.35}
}

@task(name="extract_weather_data", log_prints=True)
def extract_data(city_name: str, coords: dict, start_date: str, end_date: str):
    """
    Task 1: Extract - Извлечение данных из Open-Meteo API
    """
    print(f"Extracting data for {city_name}...")
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,relative_humidity_2m",
        "timezone": "auto"
    }
    
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data for {city_name}")
    
    data = response.json()
    
    df = pd.DataFrame({
        "time": data["hourly"]["time"],
        "temperature": data["hourly"]["temperature_2m"],
        "humidity": data["hourly"]["relative_humidity_2m"]
    })
    
    df["city"] = city_name
    df["time"] = pd.to_datetime(df["time"])
    
    print(f"Extracted {len(df)} rows for {city_name}")
    return df

@task(name="load_to_storage", log_prints=True)
def load_data(df: pd.DataFrame, city_name: str):
    """
    Task 2: Load - Сохранение данных в CSV и PostgreSQL
    """
    print(f"Loading data for {city_name}...")
    
    # Сохранение в CSV (для Dask)
    os.makedirs(DATA_DIR, exist_ok=True)
    file_path = f"{DATA_DIR}/{city_name}.csv"
    df.to_csv(file_path, index=False)
    
    # Сохранение в PostgreSQL
    save_dataframe_to_db(df, "weather_data")
    
    print(f"Loaded {len(df)} rows for {city_name} (CSV + PostgreSQL)")
    return file_path

@task(name="transform_with_dask", log_prints=True)
def transform_data():
    """
    Task 3: Transform - Распределенная обработка данных с помощью Dask
    """
    print("Starting Dask transformation...")
    
    try:
        client = Client(DASK_SCHEDULER)
    except Exception as e:
        raise Exception(f"Failed to connect to Dask cluster: {e}")
    
    # Чтение всех CSV файлов
    try:
        ddf = dd.read_csv(f"{DATA_DIR}/*.csv")
    except OSError:
        raise Exception("No data files found. Run extraction first.")
    
    # Агрегация
    aggregation = ddf.groupby("city").agg({
        "temperature": ["mean", "max", "min"],
        "humidity": "mean"
    })
    
    # Вычисление
    result = aggregation.compute()
    
    # Форматирование
    result.columns = ['_'.join(col).strip() for col in result.columns.values]
    result_for_db = result.reset_index()
    result_for_db.columns = ['city', 'temp_mean', 'temp_max', 'temp_min', 'humidity_mean']
    result_for_db['last_updated'] = datetime.now()
    
    # Сохранение агрегированных данных
    clear_table("weather_aggregated")
    save_dataframe_to_db(result_for_db, "weather_aggregated")
    
    print(f"Transformation complete. Processed {len(result_for_db)} cities.")
    return result_for_db

@flow(name="weather_etl_pipeline", log_prints=True)
def weather_etl_flow(start_date: str = "2023-01-01", end_date: str = "2023-12-31"):
    """
    Главный ETL Flow
    
    Этапы:
    1. Extract - извлечение данных из API для каждого города
    2. Load - сохранение в CSV и PostgreSQL
    3. Transform - распределенная обработка с Dask
    """
    print("="*60)
    print("Starting Weather ETL Pipeline")
    print(f"Date range: {start_date} to {end_date}")
    print("="*60)
    
    # Инициализация БД
    init_db()
    
    # Этап 1: Extract & Load для каждого города (параллельно)
    file_paths = []
    for city_name, coords in CITIES.items():
        df = extract_data(city_name, coords, start_date, end_date)
        file_path = load_data(df, city_name)
        file_paths.append(file_path)
    
    # Этап 2: Transform (распределенная обработка)
    aggregated_result = transform_data()
    
    print("="*60)
    print("ETL Pipeline completed successfully!")
    print("="*60)
    
    return {
        "status": "success",
        "files_processed": len(file_paths),
        "cities_analyzed": len(aggregated_result),
        "result": aggregated_result.to_dict(orient="records")
    }

if __name__ == "__main__":
    # Запуск flow локально
    result = weather_etl_flow("2023-01-01", "2023-01-31")
    print(result)

