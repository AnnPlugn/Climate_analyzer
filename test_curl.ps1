# Примеры PowerShell запросов для тестирования API
# Запуск: .\test_curl.ps1

$BASE_URL = "http://localhost:8000"

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Тестирование Weather Dask ETL API" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# 1. Очистка данных
Write-Host "`n1. Очистка данных..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$BASE_URL/etl/clean" -Method Delete
    $response | ConvertTo-Json -Depth 10
} catch {
    Write-Host "Ошибка: $_" -ForegroundColor Red
}

# 2. Сбор данных (быстрый тест - 1 месяц)
Write-Host "`n2. Сбор данных за январь 2023..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$BASE_URL/etl/ingest?start_date=2023-01-01&end_date=2023-01-31" -Method Post
    $response | ConvertTo-Json -Depth 10
} catch {
    Write-Host "Ошибка: $_" -ForegroundColor Red
}

Start-Sleep -Seconds 2

# 3. Анализ данных
Write-Host "`n3. Распределенная обработка данных..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$BASE_URL/etl/analyze" -Method Get
    $response | ConvertTo-Json -Depth 10
} catch {
    Write-Host "Ошибка: $_" -ForegroundColor Red
}

Write-Host "`n==========================================" -ForegroundColor Cyan
Write-Host "Тестирование завершено!" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "`nDask Dashboard: http://localhost:8787/status" -ForegroundColor Magenta
Write-Host "Swagger UI: http://localhost:8000/docs" -ForegroundColor Magenta

