import os
from typing import Iterable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# Асинхронный engine
engine = create_async_engine(
    f"postgresql+asyncpg://"
    f"{os.getenv('POSTGRES_USER', 'postgres')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'postgres')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'weather_db')}"
)


async def clear_table_async(table_name: str):
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY"))


async def save_dataframe_to_db_async(df: pd.DataFrame, table_name: str):
    """Асинхронная вставка DataFrame в таблицу."""
    if df.empty:
        return

    records: Iterable[dict] = df.to_dict(orient="records")
    columns = ", ".join(records[0].keys())
    placeholders = ", ".join([f":{col}" for col in records[0].keys()])

    query = text(
        f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    )

    async with engine.begin() as conn:
        await conn.execute(query, records)
