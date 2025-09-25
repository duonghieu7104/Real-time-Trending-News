# Simple Docker Commands

## Start Infrastructure
```powershell
docker-compose up -d
```

## Run Kenh14 Crawler
```powershell
docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/Kenh14_Crawler.py
```

## Run RSS Consumer
```powershell
docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/RSS_Consumer.py
```

## Run VnExpress Crawler
```powershell
docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/Vnexpress_Crawler.py
```

## Run Tuoitre Crawler
```powershell
docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/Tuoitre_Crawler.py
```

## Check Logs
```powershell
# Check if containers are running
docker ps

# Check specific container logs
docker logs airflow-webserver-v4
```

## Stop Everything
```powershell
docker-compose down
```

## Test Consumer (Optional)
```powershell
docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/test_consumer.py
```

