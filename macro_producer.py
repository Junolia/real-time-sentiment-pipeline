import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'reddit_sentiment'
MASSIVE_FILE = 'massive_macro_stream.csv' 

print(f"Starting High-Throughput Macro Producer...")

try:
    with open(MASSIVE_FILE, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        count = 0
        
        for row in reader:
            # Mapping Kaggle CSV columns to Spark schema
            message = {
                'subreddit': row.get('subreddit', 'unknown'),
                'author': 'kaggle_user',
                'content': row.get('body', ''), # Mapping 'body' from CSV
                'parent_post': '', 
                'type': 'comment', 
                'created_utc': time.time(), # Inject current time for real-time windowing
                'parent_id': '',
                'comment_id': f"m_{count}",
                'link_id': ''
            }
            
            if not message['content'].strip() or message['content'] == '[deleted]':
                continue
                
            producer.send(TOPIC_NAME, value=message)
            count += 1
            
            # Send approx 2000 messages per second to simulate heavy load
            if count % 100 == 0:
                time.sleep(0.05)
                if count % 10000 == 0:
                    print(f"Status: {count} records injected into Kafka")

    print("Data injection complete.")

except Exception as e:
    print(f"Error during ingestion: {e}")