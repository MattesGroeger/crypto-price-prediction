from kafka import KafkaConsumer, KafkaProducer
import json
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Download NLTK data
nltk.download('vader_lexicon')

def create_kafka_consumer():
    """Create Kafka consumer"""
    return KafkaConsumer(
        'crypto-reddit-data',
        bootstrap_servers='localhost:9092',
        group_id='sentiment_analysis_group',
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def create_kafka_producer():
    """Create Kafka producer"""
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def analyze_sentiment(text):
    """Analyze sentiment using VADER"""
    try:
        sia = SentimentIntensityAnalyzer()
        sentiment = sia.polarity_scores(text)
        return sentiment
    except Exception as e:
        logger.error(f"Error analyzing sentiment: {e}")
        return {'compound': 0.0, 'neg': 0.0, 'neu': 0.0, 'pos': 0.0}

def process_reddit_data(post):
    """Process Reddit data and extract sentiment"""
    try:
        text = f"{post['title']} {post['text']}"
        
        # Analyze post sentiment
        sentiment = analyze_sentiment(text)
        
        # Analyze comments sentiment
        comment_sentiments = []
        for comment in post.get('comments', []):
            comment_sentiment = analyze_sentiment(comment['text'])
            comment_sentiments.append({
                'comment_id': comment['id'],
                'sentiment': comment_sentiment
            })
        
        result = {
            'post_id': post['id'],
            'timestamp': datetime.utcnow().isoformat(),
            'post_sentiment': sentiment,
            'comment_sentiments': comment_sentiments,
            'overall_sentiment': sentiment['compound'],
            'source': 'reddit'
        }
        
        logger.info(f"Processed sentiment for post {post['id']}: {sentiment['compound']}")
        return result
    except Exception as e:
        logger.error(f"Error processing Reddit data: {e}")
        return None

def main():
    try:
        # Create Kafka consumer and producer
        consumer = create_kafka_consumer()
        producer = create_kafka_producer()
        
        logger.info("Starting Sentiment Analysis job...")
        
        # Process messages
        for message in consumer:
            try:
                post = message.value
                result = process_reddit_data(post)
                
                if result:
                    producer.send('crypto-sentiment-data', value=result)
                    producer.flush()
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        consumer.close()
        producer.close()
        logger.info("Sentiment Analysis job completed")

if __name__ == "__main__":
    main() 