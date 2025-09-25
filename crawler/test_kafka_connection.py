#!/usr/bin/env python3
"""
Test Kafka connection to debug the issue
"""

from kafka import KafkaProducer, KafkaConsumer
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def test_kafka_connection():
    print("🔍 Testing Kafka connection...")
    
    # Test producer connection
    try:
        print("📤 Testing producer connection...")
        producer = KafkaProducer(
            bootstrap_servers=['kafka-v4:29092'],
            request_timeout_ms=10000,  # 10 seconds timeout
            retries=3
        )
        print("✅ Producer connection successful!")
        
        # Test sending a message
        print("📨 Testing message sending...")
        producer.send('test_topic', value=b'test message')
        producer.flush()
        print("✅ Message sent successfully!")
        
        producer.close()
        
    except Exception as e:
        print(f"❌ Producer error: {e}")
        return False
    
    # Test consumer connection
    try:
        print("📥 Testing consumer connection...")
        consumer = KafkaConsumer(
            'test_topic',
            bootstrap_servers=['kafka-v4:29092'],
            auto_offset_reset='earliest',
            consumer_timeout_ms=5000
        )
        print("✅ Consumer connection successful!")
        consumer.close()
        
    except Exception as e:
        print(f"❌ Consumer error: {e}")
        return False
    
    print("🎉 All Kafka tests passed!")
    return True

if __name__ == "__main__":
    test_kafka_connection()
