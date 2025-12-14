"""
Модуль для работы с PostgreSQL базой данных
"""

import os
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd

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
    temperature = Column(Float, nullable=True)
    humidity = Column(Float, nullable=True)

# Модель для агрегированных данных
class WeatherAggregated(Base):
    __tablename__ = "weather_aggregated"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(50), nullable=False, unique=True, index=True)
    temp_mean = Column(Float, nullable=True)
    temp_max = Column(Float, nullable=True)
    temp_min = Column(Float, nullable=True)
    humidity_mean = Column(Float, nullable=True)
    last_updated = Column(DateTime, nullable=False)

# Создание движка и сессии
engine = None
SessionLocal = None

def init_db():
    """Инициализация базы данных - создание таблиц"""
    global engine, SessionLocal
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    Base.metadata.create_all(bind=engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
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

