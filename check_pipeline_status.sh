#!/bin/bash

echo "=========================================="
echo "📊 REAL-TIME NEWS PIPELINE STATUS CHECK"
echo "=========================================="
echo ""

echo "1️⃣ Checking Containers Status..."
echo "---"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "(kafka|mongo|spark|airflow)"
echo ""

echo "2️⃣ Checking Kafka Topics..."
echo "---"
docker exec kafka-v4 kafka-topics --bootstrap-server kafka-v4:29092 --list
echo ""

echo "3️⃣ Checking Kafka Messages Count..."
echo "---"
echo "raw_news topic:"
docker exec kafka-v4 kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka-v4:29092 --topic raw_news --time -1
echo ""

echo "4️⃣ Checking MongoDB Collections..."
echo "---"
echo "Raw articles:"
docker exec mongo-v4 mongosh news_db --quiet --eval "db.articles.countDocuments({})"
echo "Processed articles:"
docker exec mongo-v4 mongosh news_db --quiet --eval "db.processed_articles.countDocuments({})"
echo ""

echo "5️⃣ Checking Running Processes..."
echo "---"
echo "Raw consumer:"
docker exec airflow-scheduler-v4 bash -c "ps aux | grep raw_data_consumer | grep -v grep | wc -l"
echo "Processed consumer:"
docker exec airflow-scheduler-v4 bash -c "ps aux | grep processed_data_consumer | grep -v grep | wc -l"
echo "Spark processor:"
docker exec spark-master-v4 bash -c "ps aux | grep working_embedding_processor | grep -v grep | wc -l"
echo ""

echo "6️⃣ Checking Spark Processor Logs (last 10 lines)..."
echo "---"
docker exec spark-master-v4 bash -c "tail -10 /tmp/working_processor_new.out 2>/dev/null || echo 'No logs found'"
echo ""

echo "=========================================="
echo "✅ Status check complete!"
echo "=========================================="

