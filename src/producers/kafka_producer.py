from kafka import KafkaProducer
import json
import logging
from typing import Dict, Any, Union
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaDataProducer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all'
        )
        self.topics = {
            'binance': 'crypto-binance-data',
            'reddit': 'crypto-reddit-data',
            'coingecko': 'crypto-coingecko-data'
        }

    def _parse_data(self, data: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
        """Parse input data to ensure it's a dictionary"""
        if isinstance(data, str):
            try:
                return json.loads(data)
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing JSON string: {e}")
                return {}
        return data

    def send_binance_data(self, data: Union[str, Dict[str, Any]]):
        """Send Binance data to Kafka"""
        try:
            parsed_data = self._parse_data(data)
            self.producer.send(
                self.topics['binance'],
                value=parsed_data
            )
            self.producer.flush()
            logger.info(f"Sent Binance data for {parsed_data.get('symbol', 'unknown')}")
        except Exception as e:
            logger.error(f"Error sending Binance data: {e}")

    def send_reddit_data(self, data: Union[str, Dict[str, Any]]):
        """Send Reddit data to Kafka"""
        try:
            parsed_data = self._parse_data(data)
            self.producer.send(
                self.topics['reddit'],
                value=parsed_data
            )
            self.producer.flush()
            logger.info(f"Sent Reddit data for post {parsed_data.get('id', 'unknown')}")
        except Exception as e:
            logger.error(f"Error sending Reddit data: {e}")

    def send_coingecko_data(self, data: Union[str, Dict[str, Any]]):
        """Send CoinGecko data to Kafka"""
        try:
            parsed_data = self._parse_data(data)
            self.producer.send(
                self.topics['coingecko'],
                value=parsed_data
            )
            self.producer.flush()
            logger.info(f"Sent CoinGecko data for {parsed_data.get('symbol', 'unknown')}")
        except Exception as e:
            logger.error(f"Error sending CoinGecko data: {e}")

    def close(self):
        """Close the Kafka producer"""
        try:
            self.producer.close()
            logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {e}") 