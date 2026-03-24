import json
import time
import requests
from kafka import KafkaProducer

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define the subreddits to stream
SUBREDDITS = ['movies', 'books', 'AskReddit', 'technology']
TOPIC_NAME = 'reddit_sentiment'

def fetch_reddit_data(subreddit):
    url = f"https://www.reddit.com/r/{subreddit}/comments.json?limit=50"
    headers = {'User-Agent': 'VandyStudentProject/0.1'}
    
    try:
        response = requests.get(url, headers=headers)
        data = response.json()
        # get the list of posts
        posts = data['data']['children']
        return posts
    except Exception as e:
        print(f"Error fetching from {subreddit}: {e}")
        return []

# Creates the live stream
print("Starting the Reddit Ingestion Engine...")
while True:
    for sub in SUBREDDITS:
        posts = fetch_reddit_data(sub)
        # could have more discussions on data fetching
        # e.g. if the body len is shorter than a threshold, then omit
        for post in posts:
            item = post['data']
            
            comment_body = item.get('body', '')
            parent_topic = item.get('link_title', 'No Topic Found')
            
            message = {
                'subreddit': sub,
                'author': item.get('author'),
                'content': comment_body,
                'parent_post': parent_topic, 

                # Metadata tag to distinguish between original posts and user comments.
                # Downstream Spark jobs can use this for weighted sentiment scoring, 
                # as comments typically reflect more direct public attitudes compared to titles.
                'type': 'comment' if 'body' in item else 'post',
                
                # easier for data aggregation
                'created_utc': item.get('created_utc')
            }
            
            # Send to kafka (with confirmation in the terminal)
            try:
                producer.send(TOPIC_NAME, value=message).get(timeout=10)
                producer.flush()
                
                display_topic = parent_topic[:30]
                display_comment = comment_body[:40].replace('\n', ' ')
                print(f"[{sub.upper()}] Topic: {display_topic}... -> Comment: {display_comment}...")
                
            except Exception as e:
                print(f"Failed to push to Kafka: {e}")
    
    # Wait 30 seconds before checking for new posts again
    time.sleep(30)