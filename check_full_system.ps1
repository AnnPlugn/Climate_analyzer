# Скрипт для проверки полного цикла работы системы

Write-Host "=" * 60 -ForegroundColor Cyan
Write-Host "Проверка полного цикла работы системы" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Cyan
Write-Host ""

# 1. Проверка статуса сервисов
Write-Host "1. Проверка статуса сервисов..." -ForegroundColor Yellow
docker-compose ps
Write-Host ""

# 2. Проверка API (Swagger)
Write-Host "2. Проверка API (Swagger UI)..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8000/docs" -UseBasicParsing -TimeoutSec 5
    if ($response.StatusCode -eq 200) {
        Write-Host "   ✓ API доступен: http://localhost:8000/docs" -ForegroundColor Green
    }
} catch {
    Write-Host "   ✗ API недоступен" -ForegroundColor Red
}
Write-Host ""

# 3. Проверка данных в БД
Write-Host "3. Проверка данных в PostgreSQL..." -ForegroundColor Yellow
$weatherCount = docker-compose exec -T postgres psql -U postgres -d weather_db -c "SELECT COUNT(*) FROM weather_data;" 2>&1 | Select-String -Pattern "\d+" | ForEach-Object { $_.Matches.Value }
$aggCount = docker-compose exec -T postgres psql -U postgres -d weather_db -c "SELECT COUNT(*) FROM weather_aggregated;" 2>&1 | Select-String -Pattern "\d+" | ForEach-Object { $_.Matches.Value }

Write-Host "   Записей в weather_data: $weatherCount" -ForegroundColor $(if ($weatherCount -gt 0) { "Green" } else { "Red" })
Write-Host "   Записей в weather_aggregated: $aggCount" -ForegroundColor $(if ($aggCount -gt 0) { "Green" } else { "Yellow" })
Write-Host ""

# 4. Проверка Dask
Write-Host "4. Проверка Dask..." -ForegroundColor Yellow
try {
    $daskResponse = Invoke-WebRequest -Uri "http://localhost:8787/status" -UseBasicParsing -TimeoutSec 5
    if ($daskResponse.StatusCode -eq 200) {
        Write-Host "   ✓ Dask Dashboard доступен: http://localhost:8787/status" -ForegroundColor Green
    }
} catch {
    Write-Host "   ⚠ Dask Dashboard недоступен (может быть в процессе запуска)" -ForegroundColor Yellow
}
Write-Host ""

# 5. Проверка Streamlit
Write-Host "5. Проверка Streamlit..." -ForegroundColor Yellow
try {
    $streamlitResponse = Invoke-WebRequest -Uri "http://localhost:8501" -UseBasicParsing -TimeoutSec 5
    if ($streamlitResponse.StatusCode -eq 200) {
        Write-Host "   ✓ Streamlit доступен: http://localhost:8501" -ForegroundColor Green
    }
} catch {
    Write-Host "   ✗ Streamlit недоступен" -ForegroundColor Red
}
Write-Host ""

# 6. Рекомендации
Write-Host "=" * 60 -ForegroundColor Cyan
Write-Host "Рекомендации:" -ForegroundColor Cyan
Write-Host "=" * 60 -ForegroundColor Cyan

if ($weatherCount -eq 0) {
    Write-Host "   → Загрузите данные через API:" -ForegroundColor Yellow
    Write-Host "     Invoke-RestMethod -Uri 'http://localhost:8000/etl/ingest?start_date=2023-01-01&end_date=2023-01-31' -Method POST" -ForegroundColor White
    Write-Host ""
}

if ($aggCount -eq 0 -and $weatherCount -gt 0) {
    Write-Host "   → Запустите анализ через API:" -ForegroundColor Yellow
    Write-Host "     Invoke-RestMethod -Uri 'http://localhost:8000/etl/analyze' -Method GET" -ForegroundColor White
    Write-Host ""
}

Write-Host "   → Откройте Swagger UI: http://localhost:8000/docs" -ForegroundColor White
Write-Host "   → Откройте Streamlit: http://localhost:8501" -ForegroundColor White
Write-Host "   → Откройте Dask Dashboard: http://localhost:8787/status" -ForegroundColor White
Write-Host ""

