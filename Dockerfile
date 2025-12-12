FROM python:3.11-slim

WORKDIR /app

# Ставим системные зависимости (нужны для сборки pyarrow/pandas на некоторых архитектурах)
RUN apt-get update && apt-get install -y gcc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
# Создаем точку монтирования для данных
RUN mkdir /data

ENV PYTHONPATH=/app

