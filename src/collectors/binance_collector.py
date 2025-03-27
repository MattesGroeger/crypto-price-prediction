import os
from binance.client import Client
import json
import logging
from datetime import datetime
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BinanceCollector:
    def __init__(self, callback=None):
        self.api_key = os.getenv('BINANCE_API_KEY')
        self.api_secret = os.getenv('BINANCE_API_SECRET')
        self.client = Client(self.api_key, self.api_secret)
        self.callback = callback
        self.symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
        self.running = False
        self.thread = None

    def process_data(self, symbol, data):
        try:
            processed_data = {
                'symbol': symbol,
                'timestamp': datetime.fromtimestamp(data['closeTime'] / 1000).isoformat(),
                'open': float(data['openPrice']),
                'high': float(data['highPrice']),
                'low': float(data['lowPrice']),
                'close': float(data['lastPrice']),
                'volume': float(data['volume']),
                'source': 'binance'
            }
            
            if self.callback:
                self.callback(json.dumps(processed_data))
            logger.info(f"Processed Binance data for {processed_data['symbol']}")
                
        except Exception as e:
            logger.error(f"Error processing Binance data: {e}")

    def _collect_data(self):
        while self.running:
            try:
                for symbol in self.symbols:
                    ticker = self.client.get_ticker(symbol=symbol)
                    self.process_data(symbol, ticker)
                time.sleep(60)  # Wait 1 minute before next update
            except Exception as e:
                logger.error(f"Error collecting Binance data: {e}")
                time.sleep(60)  # Wait before retrying

    def start(self):
        try:
            self.running = True
            self.thread = threading.Thread(target=self._collect_data)
            self.thread.start()
            logger.info("Binance collector started")
            
        except Exception as e:
            logger.error(f"Error starting Binance collector: {e}")

    def stop(self):
        try:
            self.running = False
            if self.thread:
                self.thread.join()
            logger.info("Binance collector stopped")
        except Exception as e:
            logger.error(f"Error stopping Binance collector: {e}")

    def get_historical_klines(self, symbol: str, interval: str, limit: int = 100):
        """Get historical klines/candlestick data"""
        try:
            klines = self.client.get_klines(
                symbol=symbol,
                interval=interval,
                limit=limit
            )
            return klines
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return None 