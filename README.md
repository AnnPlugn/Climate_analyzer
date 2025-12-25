# Weather Big Data ETL - Анализ климатических данных

Масштабируемый сервис для анализа климатических данных с использованием микросервисной архитектуры на базе Docker Compose.

## ✨ Особенности

- ✅ **Big Data обработка**: Dask Distributed для параллельной обработки
- ✅ **Хранение данных**: PostgreSQL для структурированного хранения
- ✅ **ETL оркестрация**: Prefect для управления workflow
- ✅ **Визуализация**: Streamlit dashboard с интерактивными графиками
- ✅ **Микросервисы**: Docker Compose оркестрация

---

## 🏗️ Архитектура системы

### Компоненты:

- **Ingestion Service (FastAPI)**: Сбор данных из Open-Meteo API
- **Storage (PostgreSQL)**: Реляционная БД для структурированных данных
- **Data Lake (CSV)**: Файловое хранилище для распределенной обработки
- **Processing (Dask Cluster)**: Распределенная обработка данных
- **Orchestration (Prefect)**: Управление ETL процессами
- **Visualization (Streamlit)**: Интерактивный dashboard

### Поток данных:

1. **Extract**: FastAPI загружает данные из Open-Meteo API
2. **Load**: Данные сохраняются в CSV файлы и PostgreSQL
3. **Transform**: Dask обрабатывает данные параллельно через workers
4. **Aggregate**: Результаты сохраняются в `weather_aggregated` таблицу
5. **Visualize**: Streamlit отображает данные в интерактивном dashboard

---

## 🚀 Запуск проекта

### Шаг 1: Настройка переменных окружения

Создайте файл `.env` в корне проекта:

```env
DATABASE_USERNAME=postgres
DATABASE_PASSWORD=postgres
DATABASE_NAME=weather_db
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
DASK_SCHEDULER_ADDRESS=dask-scheduler:8786
```

### Шаг 2: Запуск всех сервисов

```powershell
docker-compose up --build
```

Или в фоновом режиме:

```powershell
docker-compose up -d --build
```

### Шаг 3: Проверка запуска

После запуска доступны следующие сервисы:

- **FastAPI Swagger UI**: http://localhost:8000/docs
- **Streamlit Dashboard**: http://localhost:8501
- **Dask Dashboard**: http://localhost:8787/status
- **Prefect UI**: http://localhost:4201
- **PostgreSQL**: localhost:5433 (внешний порт, внутри Docker сети: 5432)

### Шаг 4: Запуск Dask Workers

Для ускорения обработки данных убедитесь, что Dask workers запущены:

```powershell
docker-compose up -d dask-worker
docker-compose ps dask-worker
```

**Ожидаемый результат:** 2 workers должны быть запущены

---

## 📊 Полный цикл работы с данными

### Этап 1: Загрузка данных (Ingestion)

**Что происходит:**
- API скачивает данные о погоде из Open-Meteo API для указанных городов
- Данные сохраняются в CSV файлы в папке `data/`
- Данные также сохраняются в PostgreSQL таблицу `weather_data`

**Способы загрузки:**

#### Способ 1: Swagger UI (РЕКОМЕНДУЕТСЯ)

1. Откройте http://localhost:8000/docs
2. Найдите `POST /etl/ingest`
3. Нажмите "Try it out"
4. Измените параметры (если нужно):
   - `start_date`: `2023-01-01`
   - `end_date`: `2023-01-31`
5. Нажмите "Execute"

#### Способ 2: PowerShell

```powershell
# Загрузка данных за месяц
Invoke-RestMethod -Uri "http://localhost:8000/etl/ingest?start_date=2023-01-01&end_date=2023-01-31" -Method POST

# Загрузка данных за весь год
Invoke-RestMethod -Uri "http://localhost:8000/etl/ingest?start_date=2023-01-01&end_date=2023-12-31" -Method POST
```

#### Способ 3: Python

```python
import requests

response = requests.post(
    "http://localhost:8000/etl/ingest",
    params={
        "start_date": "2023-01-01",
        "end_date": "2023-01-31"
    }
)
print(response.json())
```

**Параметры:**
- `start_date` (опционально) - начальная дата в формате `YYYY-MM-DD` (по умолчанию: `2023-01-01`)
- `end_date` (опционально) - конечная дата в формате `YYYY-MM-DD` (по умолчанию: `2023-12-31`)
- `cities_subset` (опционально) - список городов для обработки

**Ответ:**
```json
{
  "status": "Ingestion Complete",
  "details": [
    "Saved 744 rows for London (CSV + PostgreSQL)",
    "Saved 744 rows for Paris (CSV + PostgreSQL)",
    ...
  ]
}
```

### Этап 2: Анализ данных через Dask (Transform)

**Что происходит:**
- Dask читает все CSV файлы из папки `data/` параллельно
- Workers обрабатывают данные распределенно
- Вычисляются агрегированные метрики по городам:
  - Средняя, максимальная, минимальная температура
  - Средняя влажность, давление, скорость ветра
  - Сумма осадков, снега
  - И другие метрики
- Результаты сохраняются в PostgreSQL таблицу `weather_aggregated`

**Запуск анализа:**

#### Способ 1: Swagger UI (РЕКОМЕНДУЕТСЯ)

1. Откройте http://localhost:8000/docs
2. Найдите `GET /etl/analyze`
3. Нажмите "Try it out"
4. Нажмите "Execute"
5. Дождитесь ответа (обычно 10-30 секунд с 2 workers)

#### Способ 2: PowerShell

```powershell
Invoke-RestMethod -Uri "http://localhost:8000/etl/analyze" -Method GET
```

#### Способ 3: Python

```python
import requests

response = requests.get("http://localhost:8000/etl/analyze")
print(response.json())
```

**Мониторинг обработки:**
- Откройте http://localhost:8787/status
- Вы увидите визуализацию обработки в реальном времени
- Видно, как данные распределяются между workers

**Ответ:**
```json
{
  "analysis_time_sec": 2.3456,
  "workers_count": 2,
  "data": [
    {
      "city": "London",
      "temp_mean": 10.5,
      "temp_max": 25.3,
      "temp_min": -2.1,
      "humidity_mean": 75.2,
      ...
    },
    ...
  ]
}
```

### Этап 3: Просмотр результатов

#### Проверка данных в PostgreSQL

```powershell
# Количество записей в таблицах
docker-compose exec postgres psql -U postgres -d weather_db -c "SELECT COUNT(*) FROM weather_data; SELECT COUNT(*) FROM weather_aggregated;"

# Просмотр агрегированных данных
docker-compose exec postgres psql -U postgres -d weather_db -c "SELECT * FROM weather_aggregated;"

# Просмотр структуры таблиц
docker-compose exec postgres psql -U postgres -d weather_db -c "\d weather_data"
docker-compose exec postgres psql -U postgres -d weather_db -c "\d weather_aggregated"
```

#### Получение статистики через API

```powershell
Invoke-RestMethod -Uri "http://localhost:8000/etl/stats" -Method GET
```

### Этап 4: Визуализация в Streamlit

1. Откройте http://localhost:8501
2. Нажмите **F5** (обновить страницу)
3. Проверьте вкладки:
   - **"Графики"** - временные ряды температуры и влажности
   - **"По городам"** - сравнение метрик между городами
   - **"Агрегированные данные"** - таблица с результатами из PostgreSQL
   - **"Сырые данные"** - таблица с возможностью сортировки и экспорта

---

## 🧪 Тестирование системы

### Быстрая проверка всех компонентов

#### 1. Проверка API

Откройте: **http://localhost:8000/docs**

**Что проверить:**
- Страница открывается
- Видны все endpoints (`/etl/ingest`, `/etl/analyze`, `/etl/stats`, `/etl/clean`)

#### 2. Проверка Dask Workers

```powershell
docker-compose ps dask-worker
```

**Если workers не запущены:**
```powershell
docker-compose up -d dask-worker
```

#### 3. Проверка PostgreSQL

```powershell
# Проверка подключения
docker-compose exec postgres psql -U postgres -d weather_db -c "SELECT version();"

# Проверка таблиц
docker-compose exec postgres psql -U postgres -d weather_db -c "\dt"

# Проверка данных
docker-compose exec postgres psql -U postgres -d weather_db -c "SELECT COUNT(*) FROM weather_data; SELECT COUNT(*) FROM weather_aggregated;"
```

#### 4. Проверка Streamlit

Откройте: **http://localhost:8501**

**Что проверить:**
- Страница открывается
- Видны вкладки с данными
- Графики отображаются корректно

### Полный тест цикла работы

#### Шаг 1: Загрузка данных

```powershell
Invoke-RestMethod -Uri "http://localhost:8000/etl/ingest?start_date=2023-01-01&end_date=2023-01-31" -Method POST
```

**Проверка:**
```powershell
docker-compose exec postgres psql -U postgres -d weather_db -c "SELECT COUNT(*) FROM weather_data;"
```

#### Шаг 2: Запуск анализа

```powershell
Invoke-RestMethod -Uri "http://localhost:8000/etl/analyze" -Method GET
```

**Мониторинг:**
- Откройте http://localhost:8787/status для визуализации обработки

**Проверка:**
```powershell
docker-compose exec postgres psql -U postgres -d weather_db -c "SELECT * FROM weather_aggregated;"
```

#### Шаг 3: Проверка Streamlit

1. Откройте http://localhost:8501
2. Нажмите **F5** (обновить)
3. Проверьте вкладки:
   - **"По городам"** - графики сравнения городов
   - **"Агрегированные данные"** - таблица с результатами

---

## ⚡ Оптимизация производительности

### Если обработка слишком долгая:

1. **Увеличьте количество Dask workers:**
   ```powershell
   docker-compose up -d --scale dask-worker=4
   ```

2. **Уменьшите объем данных:**
   - Загрузите данные только за 1 неделю вместо месяца:
   ```powershell
   Invoke-RestMethod -Uri "http://localhost:8000/etl/ingest?start_date=2023-01-01&end_date=2023-01-07" -Method POST
   ```

3. **Используйте Swagger UI** - там видно прогресс запроса

4. **Мониторьте Dask Dashboard** - http://localhost:8787/status

---

## 🔍 Диагностика проблем

### API не работает

```powershell
docker-compose logs api --tail 30
docker-compose restart api
```

### Dask не обрабатывает данные

```powershell
# Проверить workers
docker-compose ps dask-worker

# Запустить workers
docker-compose up -d dask-worker

# Проверить логи
docker-compose logs dask-scheduler --tail 30
```

### Данные не сохраняются в БД

```powershell
# Проверить ошибки
docker-compose logs api --tail 50 | Select-String -Pattern "Error|Exception"

# Проверить структуру таблицы
docker-compose exec postgres psql -U postgres -d weather_db -c "\d weather_aggregated"
```

### Streamlit не показывает данные

1. Обновите страницу (F5)
2. Проверьте отладочную информацию (разверните секцию "🔍 Отладочная информация")
3. Проверьте логи:
   ```powershell
   docker-compose logs streamlit --tail 30
   ```

### PostgreSQL не запускается

```powershell
# Проверить статус
docker-compose ps postgres

# Проверить логи
docker-compose logs postgres --tail 50

# Перезапустить
docker-compose restart postgres
```

---

## 📋 Доступные API Endpoints

### 1. POST /etl/ingest - Загрузка данных о погоде

**Параметры:**
- `start_date` (строка, опционально) - начальная дата в формате `YYYY-MM-DD`
- `end_date` (строка, опционально) - конечная дата в формате `YYYY-MM-DD`
- `cities_subset` (массив строк, опционально) - список городов для обработки

**Пример:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/etl/ingest?start_date=2023-01-01&end_date=2023-01-31" -Method POST
```

### 2. GET /etl/analyze - Анализ данных с помощью Dask

**Параметры:** Нет

**Пример:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/etl/analyze" -Method GET
```

### 3. GET /etl/stats - Получение статистики из PostgreSQL

**Параметры:** Нет

**Пример:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/etl/stats" -Method GET
```

### 4. DELETE /etl/clean - Очистка данных

**⚠️ ВНИМАНИЕ:** Эта операция удаляет все данные!

**Параметры:** Нет

**Пример:**
```powershell
Invoke-RestMethod -Uri "http://localhost:8000/etl/clean" -Method DELETE
```

---

## 📊 Доступные города

API поддерживает следующие города:

**Европа:**
- London, Berlin, Paris, Madrid, Moscow, Rome, Stockholm, Athens, Vienna

**Северная Америка:**
- New York, Los Angeles, Chicago, Toronto, Mexico City

**Азия и Тихий океан:**
- Tokyo, Singapore, Mumbai, Sydney, Seoul

**Южная Америка и Африка:**
- Rio de Janeiro, Sao Paulo, Cairo, Johannesburg, Cape Town

---

## 🛠️ Технологический стек

### Big Data:
- **Dask Distributed** - распределенная обработка данных

### Хранение:
- **PostgreSQL** - реляционная база данных

### ETL Оркестрация:
- **Prefect** - workflow orchestration

### Визуализация:
- **Streamlit** - интерактивный dashboard
- **Plotly** - интерактивные графики

### Backend:
- **Python 3.11**
- **FastAPI** - REST API
- **Pandas** - обработка данных
- **SQLAlchemy** - ORM для PostgreSQL

### Инфраструктура:
- **Docker Compose** - оркестрация микросервисов

---

## 📁 Структура проекта

```
weather_bigdata/
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI сервис
│   ├── database.py          # PostgreSQL модели и функции
│   ├── prefect_flow.py      # Prefect ETL workflow
│   └── streamlit_app.py     # Streamlit dashboard
├── data/                    # CSV файлы (автоматически)
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env                     # Переменные окружения
├── check_db.py             # Скрипт для проверки подключения к БД
└── README.md               # Этот файл
```

---

## 📈 Масштабирование

Для увеличения количества воркеров:

```powershell
docker-compose up -d --scale dask-worker=4
```

Или измените `replicas` в `docker-compose.yml` (требует Docker Swarm).

---

## 🔄 Использование Prefect

Prefect flow автоматически запускается при старте сервиса. Для ручного запуска:

```python
from app.prefect_flow import weather_etl_flow

# Запуск ETL процесса
result = weather_etl_flow("2023-01-01", "2023-12-31")
```

Или через Prefect UI: http://localhost:4201

---

## 📚 Полезные ссылки

- **Swagger UI**: http://localhost:8000/docs
- **API документация**: http://localhost:8000/redoc
- **Streamlit Dashboard**: http://localhost:8501
- **Dask Dashboard**: http://localhost:8787/status
- **Prefect UI**: http://localhost:4201

---

## ✅ Чек-лист проверки системы

- [ ] API доступен (http://localhost:8000/docs)
- [ ] Dask workers запущены (`docker-compose ps dask-worker`)
- [ ] PostgreSQL работает (`docker-compose ps postgres`)
- [ ] Streamlit доступен (http://localhost:8501)
- [ ] Данные в `weather_data` (> 0 записей)
- [ ] Анализ выполнен (`weather_aggregated` > 0 записей)
- [ ] Streamlit отображает данные
- [ ] Графики работают в разделе "По городам"
- [ ] Таблица данных видна в "Агрегированные данные"

---

## 🎯 Что демонстрирует проект

- **ETL пайплайн**: Extract (Open-Meteo API) → Transform (Dask) → Load (PostgreSQL + CSV)
- **Big Data технология**: Dask для распределенной обработки данных
- **Система хранения**: PostgreSQL для структурированных данных
- **ETL оркестрация**: Prefect для управления workflow
- **Визуализация**: Streamlit dashboard с интерактивными графиками
- **Микросервисы**: Разделение ответственности между компонентами
- **Docker**: Контейнеризация и оркестрация сервисов
