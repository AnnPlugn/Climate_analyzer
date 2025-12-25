import os
import requests
import pandas as pd
import numpy as np
import math
import dask.dataframe as dd
from fastapi import FastAPI, HTTPException
from dask.distributed import Client
import time
import datetime
from datetime import datetime, timedelta
from app.database import init_db, save_dataframe_to_db, get_aggregated_data, clear_table
import logging


logger = logging.getLogger("uvicorn")

app = FastAPI(title="Weather Dask ETL")

# Настройки
DASK_SCHEDULER = os.getenv("DASK_SCHEDULER_ADDRESS", "127.0.0.1:8786")
DATA_DIR = "/data"

# Инициализация базы данных при старте
init_db()

from dask.distributed import Client
import os

async def get_client_async():
    """Асинхронное получение Dask-клиента"""
    scheduler_address = os.getenv("DASK_SCHEDULER_ADDRESS", "dask-scheduler:8786")
    try:
        # Асинхронное подключение
        return await Client(scheduler_address, asynchronous=True)
    except Exception as e:
        logger.error(f"Dask connection failed: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=f"Service unavailable: Dask cluster not ready ({str(e)})"
        )

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
        try:
            response = requests.get(url, params=params, timeout=30)
        except requests.exceptions.RequestException as exc:
            summary.append(f"Error fetching data for {city_name}: {exc}")
            continue
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
    """ETL Step 2: Transform с расширенными агрегациями (асинхронная версия)"""
    start_time = time.time()
    
    # 1. Получаем асинхронный клиент
    client = await get_client_async()
    
    # 2. Читаем данные с оптимизацией
    try:
        # Используем более эффективное чтение
        ddf = dd.read_csv(
            f"{DATA_DIR}/*.csv",
            parse_dates=['time'],
            blocksize='64MB'  # Оптимальный размер блока для чтения
        )
    except OSError:
        logger.error("No CSV files found in data directory")
        raise HTTPException(
            status_code=404, 
            detail="No data found. Run /etl/ingest first."
        )
    
    # 3. Базовые агрегаты, соответствующие схеме weather_aggregated
    agg_spec = {
        "temperature": ["mean", "max", "min"],
        "apparent_temperature": "mean",
        "dewpoint_2m": "mean",
        "humidity": "mean",
        "precipitation": "sum",
        "rain": "sum",
        "showers": "sum",
        "snowfall": "sum",
        "snow_depth": "max",
        "wind_speed": "mean",
        "wind_gusts_10m": "max",
        "pressure": "mean",
        "cloud_cover": "mean",
        "cloud_cover_low": "mean",
        "cloud_cover_mid": "mean",
        "cloud_cover_high": "mean",
        "shortwave_radiation": "mean",
        "sunshine_duration": "sum",
        "uv_index": "max",
    }
    
    # Оптимизируем: группируем и агрегируем
    grouped = ddf.groupby("city")
    agg_df = grouped.agg(agg_spec)
    
    # Вычисляем все метрики параллельно
    result_future = client.compute(agg_df)
    count_future = client.compute(grouped.size())
    # Упрощаем дополнительные метрики для ускорения
    precip_future = client.compute(ddf[ddf["precipitation"] > 0.1].groupby("city").size())
    snow_future = client.compute(ddf[ddf["snowfall"] > 0.1].groupby("city").size())
    
    try:
        result, data_points, precip_days, snow_days = await client.gather(
            [result_future, count_future, precip_future, snow_future]
        )
    except Exception as e:
        logger.exception("Dask computation failed")
        raise HTTPException(
            status_code=500,
            detail=f"Analysis failed: {str(e)}"
        )
    
    # Приводим структуру к плоскому виду и ожидаемым именам колонок
    result.columns = [
        "_".join(col) if isinstance(col, tuple) else col
        for col in result.columns
    ]
    result = result.reset_index()
    rename_map = {
        "temperature_mean": "temp_mean",
        "temperature_max": "temp_max",
        "temperature_min": "temp_min",
        "apparent_temperature_mean": "apparent_temp_mean",
        "dewpoint_2m_mean": "dewpoint_mean",
        "humidity_mean": "humidity_mean",
        "precipitation_sum": "precipitation_sum",
        "rain_sum": "rain_sum",
        "showers_sum": "showers_sum",
        "snowfall_sum": "snowfall_sum",
        "snow_depth_max": "snow_depth_max",
        "wind_speed_mean": "wind_speed_mean",
        "wind_gusts_10m_max": "wind_gusts_max",
        "pressure_mean": "pressure_mean",
        "cloud_cover_mean": "cloud_cover_mean",
        "cloud_cover_low_mean": "cloud_cover_low_mean",
        "cloud_cover_mid_mean": "cloud_cover_mid_mean",
        "cloud_cover_high_mean": "cloud_cover_high_mean",
        "shortwave_radiation_mean": "shortwave_radiation_mean",
        "sunshine_duration_sum": "sunshine_hours_total",
        "uv_index_max": "uv_index_max",
    }
    result = result.rename(columns=rename_map)
    
    # Добавляем рассчитанные счетчики
    result = result.set_index("city")
    result = result.join(data_points.rename("data_points_count"), how="left")
    
    # Добавляем дополнительные метрики, если они есть
    if len(precip_days) > 0:
        result = result.join(precip_days.rename("days_with_precipitation"), how="left")
    else:
        result["days_with_precipitation"] = 0
    
    if len(snow_days) > 0:
        result = result.join(snow_days.rename("days_with_snow"), how="left")
    else:
        result["days_with_snow"] = 0
    
    result = result.reset_index()
    
    # Преобразуем секунды солнечного сияния в часы для читаемости
    if "sunshine_hours_total" in result.columns:
        result["sunshine_hours_total"] = result["sunshine_hours_total"] / 3600.0
    
    # Метаданные и безопасные значения для JSON
    result["last_updated"] = datetime.utcnow()
    result = result.replace([np.inf, -np.inf], np.nan)
    result = result.where(pd.notnull(result), None)
    
    # Сохранение в БД (синхронно)
    try:
        clear_table("weather_aggregated")
        save_dataframe_to_db(result, "weather_aggregated")
    except Exception as e:
        logger.exception("Database operation failed")
        raise HTTPException(
            status_code=500,
            detail=f"Database error: {str(e)}"
        )
    
    # Формирование ответа
    duration = time.time() - start_time
    
    # Жесткая очистка NaN/inf перед возвратом
    records = []
    for rec in result.to_dict(orient="records"):
        cleaned = {}
        for k, v in rec.items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                cleaned[k] = None
            elif isinstance(v, (np.floating, np.float32, np.float64)) and (math.isnan(float(v)) or math.isinf(float(v))):
                cleaned[k] = None
            elif isinstance(v, (pd.Timestamp, np.datetime64)):
                cleaned[k] = pd.to_datetime(v).isoformat()
            else:
                cleaned[k] = v
        records.append(cleaned)
    
    return {
        "analysis_time_sec": round(duration, 4),
        "workers_count": len(client.scheduler_info()['workers']),
        "data": records
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
        
        # Очистка данных от NaN и inf для JSON сериализации
        aggregated = aggregated.replace([np.inf, -np.inf], np.nan)
        aggregated = aggregated.fillna(None)  # Заменяем все NaN на None
        
        # Преобразуем в словари с очисткой некорректных значений
        records = []
        for rec in aggregated.to_dict(orient="records"):
            cleaned = {}
            for k, v in rec.items():
                try:
                    # Пропускаем id колонку
                    if k == 'id':
                        continue
                    
                    # Проверяем на None
                    if v is None:
                        cleaned[k] = None
                        continue
                    
                    # Проверяем на float и его подтипы
                    if isinstance(v, (float, np.floating, np.float32, np.float64)):
                        try:
                            v_float = float(v)
                            if math.isnan(v_float) or math.isinf(v_float) or not (-1e308 < v_float < 1e308):
                                cleaned[k] = None
                            else:
                                cleaned[k] = v_float
                        except (ValueError, TypeError, OverflowError):
                            cleaned[k] = None
                    # Проверяем на datetime
                    elif isinstance(v, (pd.Timestamp, np.datetime64, datetime)):
                        try:
                            cleaned[k] = pd.to_datetime(v).isoformat()
                        except:
                            cleaned[k] = None
                    # Проверяем на NaN через pandas
                    elif isinstance(v, (int, str, bool)):
                        cleaned[k] = v
                    else:
                        # Для всех остальных типов пытаемся преобразовать
                        try:
                            if pd.isna(v):
                                cleaned[k] = None
                            else:
                                cleaned[k] = v
                        except:
                            cleaned[k] = None
                except Exception:
                    cleaned[k] = None
            records.append(cleaned)
        
        return {
            "status": "success",
            "cities_count": len(aggregated),
            "data": records
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
