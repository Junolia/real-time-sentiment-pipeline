import json
import time
import requests
from kafka import KafkaProducer
import random

producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

SUBREDDITS = ['movies', 'books', 'AskReddit', 'technology']
TOPIC_NAME = 'reddit_sentiment'
sent_comment_ids = set()

def fetch_reddit_data(subreddit):
    url = f"https://www.reddit.com/r/{subreddit}/comments.json?limit=25" # 💡 减少每次抓取数量
    # 💡 关键：每次运行改一下这个版本号 (比如 0.3, 0.4)，让 Reddit 觉得是新应用
    headers = {'User-Agent': 'VandyBigDataProject/0.5 (by u/selenaxu)'} 
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            return response.json().get('data', {}).get('children', [])
        elif response.status_code == 429:
            print(f"⚠️ RATE LIMIT on r/{subreddit}. Reddit is grumpy. Sleeping 60s...")
            time.sleep(60) # 💡 遇到 429 立刻原地休息一分钟
            return []
        else:
            print(f"❌ Error {response.status_code} on r/{subreddit}")
            return []
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return []

print("Starting the 'Super Chill' Ingestion Engine...")

while True:
    for sub in SUBREDDITS:
        print(f"--- Checking r/{sub} ---")
        raw_posts = fetch_reddit_data(sub)
        
        new_count = 0
        for post in raw_posts:
            item = post['data']
            comment_id = item.get('id')

            if comment_id in sent_comment_ids:
                continue
            
            message = {
                'subreddit': sub,
                'author': item.get('author'),
                'content': item.get('body', ''),
                'parent_post': item.get('link_title', 'No Topic'), 
                'type': 'comment', 
                'created_utc': item.get('created_utc'),
                'parent_id': item.get('parent_id'),
                'comment_id': comment_id,
                'link_id': item.get('link_id')
            }
            
            producer.send(TOPIC_NAME, value=message)
            sent_comment_ids.add(comment_id)
            new_count += 1
            time.sleep(0.1) # 💡 给 Kafka 一点点呼吸空间

        print(f"✅ Streamed {new_count} new comments from r/{sub}.")
        
        # 💡 核心变动：每抓完一个 Subreddit 就歇 30-45 秒，而不是全部抓完再歇
        wait_time = random.randint(30, 45)
        print(f"Waiting {wait_time}s before next subreddit...")
        time.sleep(wait_time)

    # 内存管理
    if len(sent_comment_ids) > 5000:
        sent_comment_ids.clear()