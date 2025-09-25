
docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/Kenh14_Crawler.py
docker exec -it airflow-webserver-v4 python /opt/airflow/crawler/RSS_Consumer.py



vào http://localhost:8085/home
run tất cả dag thì sẽ cào bài báo push vào topic `raw_news` -> mongo