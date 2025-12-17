import os
import requests
import pandas as pd
import dask.dataframe as dd
from fastapi import FastAPI, HTTPException
from dask.distributed import Client
import time
import datetime
from datetime import datetime, timedelta
from app.database import init_db, save_dataframe_to_db, get_aggregated_data, clear_table

app = FastAPI(title="Weather Dask ETL")

# Настройки
DASK_SCHEDULER = os.getenv("DASK_SCHEDULER_ADDRESS", "127.0.0.1:8786")
DATA_DIR = "/data"

# Инициализация базы данных при старте
init_db()

# Координаты городов для анализа
CITIES = {
    # Европа
    "London": {"lat": 51.50, "lon": -0.12},
    "Berlin": {"lat": 52.52, "lon": 13.41},
    "Paris": {"lat": 48.85, "lon": 2.35},
    "Madrid": {"lat": 40.41, "lon": -3.70},
    "Moscow": {"lat": 55.75, "lon": 37.61},
    "Rome": {"lat": 41.90, "lon": 12.49},
    "Stockholm": {"lat": 59.32, "lon": 18.06},
    "Athens": {"lat": 37.98, "lon": 23.72},
    "Vienna": {"lat": 48.20, "lon": 16.37},
    
    # Северная Америка
    "New York": {"lat": 40.71, "lon": -74.01},
    "Los Angeles": {"lat": 34.05, "lon": -118.24},
    "Chicago": {"lat": 41.87, "lon": -87.62},
    "Toronto": {"lat": 43.65, "lon": -79.38},
    "Mexico City": {"lat": 19.43, "lon": -99.13},
    
    # Азия и Тихий океан
    "Tokyo": {"lat": 35.68, "lon": 139.69},
    "Singapore": {"lat": 1.35, "lon": 103.82},
    "Mumbai": {"lat": 19.07, "lon": 72.87},
    "Sydney": {"lat": -33.87, "lon": 151.21},
    "Seoul": {"lat": 37.56, "lon": 126.97},
    
    # Южная Америка и Африка
    "Rio de Janeiro": {"lat": -22.90, "lon": -43.17},
    "Sao Paulo": {"lat": -23.55, "lon": -46.63},
    "Cairo": {"lat": 30.04, "lon": 31.23},
    "Johannesburg": {"lat": -26.20, "lon": 28.04},
    "Cape Town": {"lat": -33.92, "lon": 18.42}
}

def get_client():
    try:
        return Client(DASK_SCHEDULER)
    except Exception as e:
        print(f"Cluster not ready: {e}")
        return None

# app/main.py (обновленная функция ingest_data)

@app.post("/etl/ingest")
async def ingest_data(start_date: str = "2023-01-01", end_date: str = "2023-12-31", cities_subset: list = None):
    """
    ETL Step 1: Extract & Load с расширенным набором параметров
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    summary = []
    
    # Если указан подмножество городов, используем их, иначе все города
    cities_to_process = {k: v for k, v in CITIES.items() if k in cities_subset} if cities_subset else CITIES
    
    for city_name, coords in cities_to_process.items():
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": "temperature_2m,apparent_temperature,dewpoint_2m,relative_humidity_2m,precipitation,rain,showers,snowfall,snow_depth,weather_code,wind_speed_10m,wind_gusts_10m,wind_direction_10m,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,shortwave_radiation,direct_radiation,diffuse_radiation,uv_index,sunshine_duration,soil_temperature_0_to_7cm,soil_moisture_0_to_7cm,is_day",
            "timezone": "auto"
        }
        
        # Запрос к API
        response = requests.get(url, params=params)
        if response.status_code != 200:
            summary.append(f"Error fetching data for {city_name}: {response.status_code}")
            continue
            
        data = response.json()
        
        # Создание DataFrame с расширенным набором параметров
        df = pd.DataFrame({
            "time": data["hourly"]["time"],
            "temperature": data["hourly"]["temperature_2m"],
            "apparent_temperature": data["hourly"]["apparent_temperature"],
            "dewpoint_2m": data["hourly"]["dewpoint_2m"],
            "humidity": data["hourly"]["relative_humidity_2m"],
            "precipitation": data["hourly"]["precipitation"],
            "rain": data["hourly"]["rain"],
            "showers": data["hourly"]["showers"],
            "snowfall": data["hourly"]["snowfall"],
            "snow_depth": data["hourly"].get("snow_depth"),  # может отсутствовать
            "weather_code": data["hourly"]["weather_code"],
            "wind_speed": data["hourly"]["wind_speed_10m"],
            "wind_gusts_10m": data["hourly"]["wind_gusts_10m"],
            "wind_direction": data["hourly"]["wind_direction_10m"],
            "pressure": data["hourly"]["surface_pressure"],
            "cloud_cover": data["hourly"]["cloud_cover"],
            "cloud_cover_low": data["hourly"]["cloud_cover_low"],
            "cloud_cover_mid": data["hourly"]["cloud_cover_mid"],
            "cloud_cover_high": data["hourly"]["cloud_cover_high"],
            "shortwave_radiation": data["hourly"]["shortwave_radiation"],
            "direct_radiation": data["hourly"]["direct_radiation"],
            "diffuse_radiation": data["hourly"]["diffuse_radiation"],
            "uv_index": data["hourly"]["uv_index"],
            "sunshine_duration": data["hourly"]["sunshine_duration"],
            "soil_temperature_0_to_7cm": data["hourly"].get("soil_temperature_0_to_7cm"),
            "soil_moisture_0_to_7cm": data["hourly"].get("soil_moisture_0_to_7cm"),
            "is_day": data["hourly"]["is_day"]
        })
        
        # Обогащение данных
        df["city"] = city_name
        df["time"] = pd.to_datetime(df["time"])
        df["data_source"] = "open-meteo"
        df["last_updated"] = datetime.utcnow()
        
        # Очистка данных: замена None/NaN на None для PostgreSQL
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].where(pd.notnull(df[col]), None)
        
        # Сохранение в CSV
        file_path = f"{DATA_DIR}/{city_name}.csv"
        df.to_csv(file_path, index=False)
        
        # Сохранение в PostgreSQL
        save_dataframe_to_db(df, "weather_data")
        
        summary.append(f"Saved {len(df)} rows for {city_name} (CSV + PostgreSQL)")
    
    return {"status": "Ingestion Complete", "details": summary}


@app.get("/etl/analyze")
async def analyze_weather():
    """
    ETL Step 2: Transform с расширенными агрегациями
    """
    client = get_client()
    
    try:
        ddf = dd.read_csv(f"{DATA_DIR}/*.csv")
    except OSError:
        raise HTTPException(status_code=404, detail="No data found. Run /etl/ingest first.")
    
    # Расширенная агрегация данных
    agg_operations = {
        "temperature": ["mean", "max", "min"],
        "apparent_temperature": ["mean"],
        "dewpoint_2m": ["mean"],
        "humidity": ["mean"],
        "precipitation": ["sum"],
        "rain": ["sum"],
        "showers": ["sum"],
        "snowfall": ["sum"],
        "wind_speed": ["mean"],
        "wind_gusts_10m": ["max", "mean"],
        "pressure": ["mean"],
        "cloud_cover": ["mean"],
        "weather_code": lambda x: x.mode().iloc[0] if not x.mode().empty else None,
        "shortwave_radiation": ["mean"],
        "sunshine_duration": ["sum"],
        "uv_index": ["max"],
    }
    
    # Группировка и агрегация
    aggregation = ddf.groupby("city").agg(agg_operations)
    
    # Вычисление
    start = time.time()
    result = aggregation.compute()
    duration = time.time() - start
    
    # Форматирование результата
    result.columns = ['_'.join(col).strip() for col in result.columns.values]
    result_for_db = result.reset_index()
    
    # Добавление дополнительных вычисляемых полей
    result_for_db['last_updated'] = datetime.utcnow()
    result_for_db['data_points_count'] = ddf.groupby('city').size().compute().values
    result_for_db['days_with_precipitation'] = ddf[ddf['precipitation'] > 0.1].groupby('city').size().compute().values
    result_for_db['days_with_snow'] = ddf[ddf['snowfall'] > 0.1].groupby('city').size().compute().values
    
    # Переименование колонок для соответствия структуре БД
    column_mapping = {
        'city': 'city',
        'temperature_mean': 'temp_mean',
        'temperature_max': 'temp_max',
        'temperature_min': 'temp_min',
        'apparent_temperature_mean': 'apparent_temp_mean',
        'dewpoint_2m_mean': 'dewpoint_mean',
        'humidity_mean': 'humidity_mean',
        'precipitation_sum': 'precipitation_sum',
        'rain_sum': 'rain_sum',
        'showers_sum': 'showers_sum',
        'snowfall_sum': 'snowfall_sum',
        'wind_speed_mean': 'wind_speed_mean',
        'wind_gusts_10m_max': 'wind_gusts_max',
        'pressure_mean': 'pressure_mean',
        'cloud_cover_mean': 'cloud_cover_mean',
        'weather_code_<lambda>': 'weather_code_most_common',
        'shortwave_radiation_mean': 'shortwave_radiation_mean',
        'sunshine_duration_sum': 'sunshine_hours_total',
        'uv_index_max': 'uv_index_max',
    }
    
    # Применение маппинга имен колонок
    result_for_db = result_for_db.rename(columns=column_mapping)
    
    # Очистка и сохранение агрегированных данных
    clear_table("weather_aggregated")
    save_dataframe_to_db(result_for_db, "weather_aggregated")
    
    return {
        "analysis_time_sec": round(duration, 4),
        "workers_count": len(client.scheduler_info()['workers']) if client else 0,
        "data": result_for_db.to_dict(orient="records")
    }

@app.delete("/etl/clean")
async def clean_data():
    """Очистка скачанных данных (CSV и PostgreSQL)"""
    import glob
    files = glob.glob(f"{DATA_DIR}/*.csv")
    for f in files:
        os.remove(f)
    
    # Очистка базы данных
    clear_table("weather_data")
    clear_table("weather_aggregated")
    
    return {"message": f"Deleted {len(files)} CSV files and cleared PostgreSQL tables"}

@app.get("/etl/stats")
async def get_stats():
    """Получение статистики из PostgreSQL"""
    try:
        aggregated = get_aggregated_data()
        return {
            "status": "success",
            "cities_count": len(aggregated),
            "data": aggregated.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

