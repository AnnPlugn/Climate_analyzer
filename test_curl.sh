#!/bin/bash
# Примеры curl запросов для тестирования API
# Для Windows PowerShell см. test_curl.ps1

BASE_URL="http://localhost:8000"

echo "=========================================="
echo "Тестирование Weather Dask ETL API"
echo "=========================================="

# 1. Очистка данных
echo -e "\n1. Очистка данных..."
curl -X DELETE "$BASE_URL/etl/clean" | jq .

# 2. Сбор данных (быстрый тест - 1 месяц)
echo -e "\n2. Сбор данных за январь 2023..."
curl -X POST "$BASE_URL/etl/ingest?start_date=2023-01-01&end_date=2023-01-31" | jq .

sleep 2

# 3. Анализ данных
echo -e "\n3. Распределенная обработка данных..."
curl -X GET "$BASE_URL/etl/analyze" | jq .

echo -e "\n=========================================="
echo "Тестирование завершено!"
echo "=========================================="

