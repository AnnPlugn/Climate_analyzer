"""
Модуль для работы с PostgreSQL базой данных с автоматическим обновлением схемы
"""

import os
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, text, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
from datetime import datetime

# Настройки подключения
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Создание базового класса для моделей
Base = declarative_base()

# Модель данных
class WeatherData(Base):
    __tablename__ = "weather_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(50), nullable=False, index=True)
    time = Column(DateTime, nullable=False, index=True)
    
    # Основные метеопараметры
    temperature = Column(Float, nullable=True)  # температура на 2м
    apparent_temperature = Column(Float, nullable=True)  # ощущаемая температура
    dewpoint_2m = Column(Float, nullable=True)  # точка росы
    humidity = Column(Float, nullable=True)  # относительная влажность
    
    # Осадки
    precipitation = Column(Float, nullable=True)  # общее количество осадков
    rain = Column(Float, nullable=True)  # количество дождя
    showers = Column(Float, nullable=True)  # ливни
    snowfall = Column(Float, nullable=True)  # количество снега
    snow_depth = Column(Float, nullable=True)  # глубина снежного покрова
    
    # Ветер
    wind_speed = Column(Float, nullable=True)  # скорость ветра на 10м
    wind_gusts_10m = Column(Float, nullable=True)  # порывы ветра
    wind_direction = Column(Float, nullable=True)  # направление ветра
    
    # Давление и облачность
    pressure = Column(Float, nullable=True)  # давление на уровне поверхности
    cloud_cover = Column(Float, nullable=True)  # общая облачность
    cloud_cover_low = Column(Float, nullable=True)  # низкая облачность
    cloud_cover_mid = Column(Float, nullable=True)  # средняя облачность
    cloud_cover_high = Column(Float, nullable=True)  # высокая облачность
    
    # Солнечная радиация и УФ
    shortwave_radiation = Column(Float, nullable=True)  # солнечная радиация
    direct_radiation = Column(Float, nullable=True)  # прямая солнечная радиация
    diffuse_radiation = Column(Float, nullable=True)  # рассеянная радиация
    uv_index = Column(Float, nullable=True)  # индекс ультрафиолета
    sunshine_duration = Column(Float, nullable=True)  # продолжительность солнечного сияния
    
    # Параметры почвы
    soil_temperature_0_to_7cm = Column(Float, nullable=True)  # температура почвы
    soil_moisture_0_to_7cm = Column(Float, nullable=True)  # влажность почвы
    
    # Коды погоды
    weather_code = Column(Integer, nullable=True)  # WMO код погодного явления
    
    # Качество данных
    is_day = Column(Integer, nullable=True)  # 1 - день, 0 - ночь
    data_source = Column(String(20), default="open-meteo")  # источник данных
    last_updated = Column(DateTime, default=datetime.utcnow)


# Модель для агрегированных данных
class WeatherAggregated(Base):
    __tablename__ = "weather_aggregated"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(50), nullable=False, unique=True, index=True)
    
    # Температурные метрики
    temp_mean = Column(Float, nullable=True)
    temp_max = Column(Float, nullable=True)
    temp_min = Column(Float, nullable=True)
    apparent_temp_mean = Column(Float, nullable=True)
    dewpoint_mean = Column(Float, nullable=True)
    
    # Влажность и осадки
    humidity_mean = Column(Float, nullable=True)
    precipitation_sum = Column(Float, nullable=True)
    rain_sum = Column(Float, nullable=True)
    showers_sum = Column(Float, nullable=True)
    snowfall_sum = Column(Float, nullable=True)
    snow_depth_max = Column(Float, nullable=True)
    
    # Ветер
    wind_speed_mean = Column(Float, nullable=True)
    wind_gusts_max = Column(Float, nullable=True)
    wind_direction_predominant = Column(Float, nullable=True)
    
    # Давление и облачность
    pressure_mean = Column(Float, nullable=True)
    cloud_cover_mean = Column(Float, nullable=True)
    cloud_cover_low_mean = Column(Float, nullable=True)
    cloud_cover_mid_mean = Column(Float, nullable=True)
    cloud_cover_high_mean = Column(Float, nullable=True)
    
    # Солнечная активность
    shortwave_radiation_mean = Column(Float, nullable=True)
    sunshine_hours_total = Column(Float, nullable=True)
    uv_index_max = Column(Float, nullable=True)
    
    # Экстремальные погодные явления
    weather_code_most_common = Column(Integer, nullable=True)
    days_with_precipitation = Column(Integer, nullable=True)
    days_with_snow = Column(Integer, nullable=True)
    
    # Сезонные показатели
    heating_degree_days = Column(Float, nullable=True)
    cooling_degree_days = Column(Float, nullable=True)
    
    # Данные по периодам суток
    daytime_temp_mean = Column(Float, nullable=True)
    nighttime_temp_mean = Column(Float, nullable=True)
    
    # Статистика по месяцам (пример для января)
    jan_temp_mean = Column(Float, nullable=True)
    jul_temp_mean = Column(Float, nullable=True)
    
    # Метаданные
    data_points_count = Column(Integer, nullable=True)  # количество точек данных
    data_coverage_percent = Column(Float, nullable=True)  # процент покрытия данных
    last_updated = Column(DateTime, nullable=False)

# Создание движка и сессии
engine = None
SessionLocal = None

def get_engine():
    """Получение или создание engine при необходимости"""
    global engine
    if engine is None:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    return engine

def update_table_schema():
    """Автоматическое обновление схемы таблиц в соответствии с моделями"""
    inspector = inspect(get_engine())
    conn = get_engine().connect()
    
    try:
        # Обновление для таблицы weather_data
        if 'weather_data' in inspector.get_table_names():
            # Получаем текущие колонки в таблице
            current_columns = [col['name'] for col in inspector.get_columns('weather_data')]
            
            # Колонки, которые должны быть в таблице согласно модели
            expected_columns = {
                'city': String(50),
                'time': DateTime,
                'temperature': Float,
                'humidity': Float,
                'precipitation': Float,
                'wind_speed': Float,
                'wind_direction': Float,
                'pressure': Float,
                'cloud_cover': Float
            }
            
            # Добавляем отсутствующие колонки
            for col_name, col_type in expected_columns.items():
                if col_name not in current_columns:
                    print(f"Adding missing column {col_name} to weather_data")
                    # Определяем правильный SQL тип для колонки
                    sql_type = "VARCHAR(50)" if isinstance(col_type, String) else \
                              "TIMESTAMP" if isinstance(col_type, DateTime) else \
                              "DOUBLE PRECISION"
                    conn.execute(text(f"ALTER TABLE weather_data ADD COLUMN {col_name} {sql_type}"))
                    conn.commit()
        
        # Обновление для таблицы weather_aggregated
        if 'weather_aggregated' in inspector.get_table_names():
            current_columns = [col['name'] for col in inspector.get_columns('weather_aggregated')]
            
            expected_columns = {
                'city': String(50),
                'temp_mean': Float,
                'temp_max': Float,
                'temp_min': Float,
                'humidity_mean': Float,
                'precipitation_sum': Float,
                'wind_speed_mean': Float,
                'pressure_mean': Float,
                'cloud_cover_mean': Float,
                'last_updated': DateTime
            }
            
            for col_name, col_type in expected_columns.items():
                if col_name not in current_columns:
                    print(f"Adding missing column {col_name} to weather_aggregated")
                    sql_type = "VARCHAR(50)" if isinstance(col_type, String) else \
                              "TIMESTAMP" if isinstance(col_type, DateTime) else \
                              "DOUBLE PRECISION"
                    conn.execute(text(f"ALTER TABLE weather_aggregated ADD COLUMN {col_name} {sql_type}"))
                    conn.commit()
                    
    finally:
        conn.close()

def init_db():
    """Инициализация базы данных - создание таблиц и обновление схемы"""
    global engine, SessionLocal
    
    engine = get_engine()
    Base.metadata.create_all(bind=engine)  # Создает таблицы если их нет
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Важно: обновляем схему после создания таблиц
    update_table_schema()
    
    return engine, SessionLocal

def get_session():
    """Получение сессии базы данных"""
    if SessionLocal is None:
        init_db()
    return SessionLocal()

def save_dataframe_to_db(df: pd.DataFrame, table_name: str = "weather_data"):
    """
    Сохранение DataFrame в PostgreSQL
    
    Args:
        df: DataFrame с данными
        table_name: имя таблицы (weather_data или weather_aggregated)
    """
    if engine is None:
        init_db()
    
    # Автоматически обновляем схему перед сохранением данных
    update_table_schema()
    
    # Используем pandas to_sql для эффективной вставки
    df.to_sql(
        table_name,
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )

def load_data_from_db(city: str = None, limit: int = None) -> pd.DataFrame:
    """
    Загрузка данных из базы
    
    Args:
        city: фильтр по городу (опционально)
        limit: ограничение количества строк
    
    Returns:
        DataFrame с данными
    """
    if engine is None:
        init_db()
    
    query = "SELECT * FROM weather_data"
    if city:
        query += f" WHERE city = '{city}'"
    query += " ORDER BY time DESC"
    if limit:
        query += f" LIMIT {limit}"
    
    return pd.read_sql(query, engine)

def get_aggregated_data() -> pd.DataFrame:
    """Получение агрегированных данных"""
    if engine is None:
        init_db()
    
    query = "SELECT * FROM weather_aggregated ORDER BY city"
    return pd.read_sql(query, engine)

def clear_table(table_name: str = "weather_data"):
    """Очистка таблицы"""
    if engine is None:
        init_db()
    
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
        conn.commit()