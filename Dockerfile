FROM python:3.11-slim

WORKDIR /app

# Ставим системные зависимости
# gcc - для сборки некоторых пакетов
# postgresql-client - для проверки БД через psql (опционально)
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY check_db.py ./

# Создаем точку монтирования для данных
RUN mkdir /data

ENV PYTHONPATH=/app

