import os
from dotenv import load_dotenv
import logging
import threading
import time
import signal
import sys

from collectors.binance_collector import BinanceCollector
from collectors.reddit_collector import RedditCollector
from collectors.coingecko_collector import CoinGeckoCollector
from producers.kafka_producer import KafkaDataProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Load environment variables
    load_dotenv()
    
    # Create Kafka producer
    producer = KafkaDataProducer()
    
    # Create collectors
    collectors = [
        BinanceCollector(callback=producer.send_binance_data),
        RedditCollector(callback=producer.send_reddit_data),
        CoinGeckoCollector(callback=producer.send_coingecko_data)
    ]
    
    # Handle graceful shutdown
    def signal_handler(signum, frame):
        logger.info("Shutting down collectors...")
        for collector in collectors:
            collector.stop()
        producer.close()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start all collectors
        for collector in collectors:
            collector.start()
        logger.info("All collectors started")
        
        # Keep the main thread alive
        while True:
            time.sleep(1)
            
    except Exception as e:
        logger.error(f"Error in main: {e}")
        for collector in collectors:
            collector.stop()
        producer.close()
        sys.exit(1)

if __name__ == "__main__":
    main() 