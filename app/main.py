import os
import requests
import pandas as pd
import dask.dataframe as dd
from fastapi import FastAPI, HTTPException
from dask.distributed import Client
import time

app = FastAPI(title="Weather Dask ETL")

# Настройки
DASK_SCHEDULER = os.getenv("DASK_SCHEDULER_ADDRESS", "127.0.0.1:8786")
DATA_DIR = "/data"

# Координаты городов для анализа
CITIES = {
    "London": {"lat": 51.50, "lon": -0.12},
    "Berlin": {"lat": 52.52, "lon": 13.41},
    "Madrid": {"lat": 40.41, "lon": -3.70},
    "Moscow": {"lat": 55.75, "lon": 37.61},
    "Paris":  {"lat": 48.85, "lon": 2.35}
}

def get_client():
    try:
        return Client(DASK_SCHEDULER)
    except Exception as e:
        print(f"Cluster not ready: {e}")
        return None

@app.post("/etl/ingest")
async def ingest_data(start_date: str = "2020-01-01", end_date: str = "2023-12-31"):
    """
    ETL Step 1: Extract & Load
    Скачивает данные из Open-Meteo для списка городов и сохраняет их в CSV.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    summary = []

    for city_name, coords in CITIES.items():
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": "temperature_2m,relative_humidity_2m",
            "timezone": "auto"
        }
        
        # Запрос к API
        response = requests.get(url, params=params)
        if response.status_code != 200:
            continue
            
        data = response.json()
        
        # Конвертация JSON -> Pandas
        df = pd.DataFrame({
            "time": data["hourly"]["time"],
            "temperature": data["hourly"]["temperature_2m"],
            "humidity": data["hourly"]["relative_humidity_2m"]
        })
        
        # Обогащение данных (Data Enrichment): добавляем колонку города
        # Это важно, чтобы Dask потом мог сгруппировать данные по городам
        df["city"] = city_name
        
        # Сохранение (Partitioning): каждый город в свой файл
        file_path = f"{DATA_DIR}/{city_name}.csv"
        df.to_csv(file_path, index=False)
        
        summary.append(f"Saved {len(df)} rows for {city_name}")

    return {"status": "Ingestion Complete", "details": summary}

@app.get("/etl/analyze")
async def analyze_weather():
    """
    ETL Step 2: Transform
    Использует Dask Cluster для обработки всех скачанных файлов одновременно.
    """
    client = get_client()
    
    # 1. Lazy Read: Читаем все CSV файлы в папке как один гигантский датасет
    # Dask сам разберется, сколько там файлов
    try:
        ddf = dd.read_csv(f"{DATA_DIR}/*.csv")
    except OSError:
        raise HTTPException(status_code=404, detail="No data found. Run /etl/ingest first.")

    # 2. Определение графа вычислений
    # Задача: Найти среднюю температуру и макс. влажность для каждого города
    aggregation = ddf.groupby("city").agg({
        "temperature": ["mean", "max", "min"],
        "humidity": "mean"
    })

    # 3. Distributed Compute: Отправка задачи на воркеры
    start = time.time()
    result = aggregation.compute() # Здесь происходит магия
    duration = time.time() - start

    # Форматирование результата для JSON ответа
    # result - это обычный pandas dataframe (уже маленький)
    result.columns = ['_'.join(col).strip() for col in result.columns.values]
    result_dict = result.reset_index().to_dict(orient="records")

    return {
        "analysis_time_sec": round(duration, 4),
        "workers_count": len(client.scheduler_info()['workers']),
        "data": result_dict
    }

@app.delete("/etl/clean")
async def clean_data():
    """Очистка скачанных данных"""
    import glob
    files = glob.glob(f"{DATA_DIR}/*.csv")
    for f in files:
        os.remove(f)
    return {"message": f"Deleted {len(files)} files"}

