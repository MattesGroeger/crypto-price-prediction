from pycoingecko import CoinGeckoAPI
import json
import logging
from datetime import datetime
import time
from typing import Dict, Any, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CoinGeckoCollector:
    def __init__(self, callback=None):
        self.cg = CoinGeckoAPI()
        self.symbols = ['bitcoin', 'ethereum', 'binancecoin']
        self.callback = callback
        self.running = False

    def process_price(self, coin_id, price_data):
        try:
            data = {
                'symbol': coin_id,
                'timestamp': datetime.utcnow().isoformat(),
                'price': price_data[coin_id]['usd'],
                'price_change_24h': price_data[coin_id].get('usd_24h_change', 0),
                'market_cap': price_data[coin_id].get('usd_market_cap', 0),
                'source': 'coingecko'
            }
            
            if self.callback:
                self.callback(json.dumps(data))
            logger.info(f"Processed CoinGecko data for {data['symbol']}")
            
        except Exception as e:
            logger.error(f"Error processing CoinGecko price data: {e}")

    def start(self):
        self.running = True
        logger.info("Starting CoinGecko collector")
        
        try:
            while self.running:
                price_data = self.cg.get_price(ids=self.symbols, vs_currencies='usd', include_24hr_change=True, include_market_cap=True)
                
                for coin_id in self.symbols:
                    if not self.running:
                        break
                    self.process_price(coin_id, price_data)
                
                time.sleep(60)  # Wait 1 minute before next update
                
        except Exception as e:
            logger.error(f"Error in CoinGecko collector: {e}")
            self.stop()

    def stop(self):
        self.running = False
        logger.info("CoinGecko collector stopped")

    def get_current_price(self, coin_id: str) -> Dict[str, Any]:
        """Get current price and market data for a specific coin"""
        try:
            data = self.cg.get_price(ids=coin_id, vs_currencies='usd')
            return {
                'coin_id': coin_id,
                'price': data[coin_id]['usd'],
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error fetching price for {coin_id}: {e}")
            return None

    def get_market_data(self, coin_id: str) -> Dict[str, Any]:
        """Get detailed market data for a specific coin"""
        try:
            data = self.cg.get_coin_by_id(coin_id)
            return {
                'coin_id': coin_id,
                'name': data['name'],
                'symbol': data['symbol'],
                'current_price': data['market_data']['current_price']['usd'],
                'market_cap': data['market_data']['market_cap']['usd'],
                'total_volume': data['market_data']['total_volume']['usd'],
                'price_change_percentage_24h': data['market_data']['price_change_percentage_24h'],
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            logger.error(f"Error fetching market data for {coin_id}: {e}")
            return None

    def get_historical_data(self, coin_id: str, days: int = 1) -> List[Dict[str, Any]]:
        """Get historical price data for a specific coin"""
        try:
            data = self.cg.get_coin_market_chart_by_id(
                id=coin_id,
                vs_currency='usd',
                days=days
            )
            return [{
                'coin_id': coin_id,
                'timestamp': datetime.fromtimestamp(price[0]/1000).isoformat(),
                'price': price[1],
                'volume': volume[1]
            } for price, volume in zip(data['prices'], data['total_volumes'])]
        except Exception as e:
            logger.error(f"Error fetching historical data for {coin_id}: {e}")
            return []

    def stream_prices(self, callback, interval: int = 60):
        """Stream price updates for configured coins"""
        while True:
            try:
                for coin_id in self.symbols:
                    data = self.get_current_price(coin_id)
                    if data:
                        callback(data)
                time.sleep(interval)
            except Exception as e:
                logger.error(f"Error in price stream: {e}")
                time.sleep(interval) 