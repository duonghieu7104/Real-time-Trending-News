# Simple PowerShell script to run crawlers and consumer
# Much shorter and simpler commands!

Write-Host "🚀 Simple Real-time Trending News Setup" -ForegroundColor Green
Write-Host "=" * 50

# Step 1: Start all infrastructure
Write-Host "📦 Starting infrastructure..." -ForegroundColor Yellow
docker-compose up -d

# Wait for services to be ready
Write-Host "⏳ Waiting for services..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

Write-Host ""
Write-Host "✅ Infrastructure ready!" -ForegroundColor Green
Write-Host ""
Write-Host "🎯 Now you can run these simple commands:" -ForegroundColor Cyan
Write-Host ""
Write-Host "Run Kenh14 Crawler:" -ForegroundColor White
Write-Host "  docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/Kenh14_Crawler.py" -ForegroundColor Gray
Write-Host ""
Write-Host "Run RSS Consumer:" -ForegroundColor White
Write-Host "  docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/RSS_Consumer.py" -ForegroundColor Gray
Write-Host ""
Write-Host "Run VnExpress Crawler:" -ForegroundColor White
Write-Host "  docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/Vnexpress_Crawler.py" -ForegroundColor Gray
Write-Host ""
Write-Host "Run Tuoitre Crawler:" -ForegroundColor White
Write-Host "  docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/Tuoitre_Crawler.py" -ForegroundColor Gray
Write-Host ""
Write-Host "Check logs:" -ForegroundColor White
Write-Host "  docker logs airflow-webserver-v4" -ForegroundColor Gray
Write-Host ""
Write-Host "Stop everything:" -ForegroundColor White
Write-Host "  docker-compose down" -ForegroundColor Gray
