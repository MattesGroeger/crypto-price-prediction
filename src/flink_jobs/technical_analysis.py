from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd
import pandas_ta as ta
import logging
from datetime import datetime
from collections import deque
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global variables for historical data
HISTORY_SIZE = 100  # Number of data points to keep for each symbol
price_history = {}  # Dictionary to store historical data for each symbol

def create_kafka_consumer():
    """Create Kafka consumer"""
    return KafkaConsumer(
        'crypto-binance-data',
        bootstrap_servers='localhost:9092',
        group_id='technical_analysis_group',
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def create_kafka_producer():
    """Create Kafka producer"""
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def calculate_technical_indicators(data):
    """Calculate technical indicators"""
    try:
        # Log incoming data for debugging
        logger.info(f"Received data: {data}")
        
        # Check if data is valid
        if not data or not isinstance(data, dict):
            logger.error("Invalid input data: data is None or not a dictionary")
            return None
            
        # Ensure required fields are present
        required_fields = ['symbol', 'open', 'high', 'low', 'close', 'volume']
        missing_fields = [field for field in required_fields if field not in data]
        if missing_fields:
            logger.error(f"Missing required fields in data: {missing_fields}")
            logger.error(f"Available fields: {list(data.keys())}")
            return None
            
        # Initialize or update price history for the symbol
        symbol = data['symbol']
        if symbol not in price_history:
            price_history[symbol] = deque(maxlen=HISTORY_SIZE)
            
        # Add new data point to history
        price_history[symbol].append({
            'open': float(data['open']),
            'high': float(data['high']),
            'low': float(data['low']),
            'close': float(data['close']),
            'volume': float(data['volume'])
        })
        
        # Check if we have enough data points
        if len(price_history[symbol]) < 5:  # Reduced from 20 to 5 for faster processing
            logger.info(f"Not enough data points for {symbol}. Need at least 5, got {len(price_history[symbol])}")
            return None
            
        # Convert history to DataFrame
        df = pd.DataFrame(list(price_history[symbol]))
        
        # Calculate indicators with error handling
        try:
            # Simple Moving Average
            df['sma_5'] = df['close'].rolling(window=5).mean()
            
            # Exponential Moving Average
            df['ema_5'] = df['close'].ewm(span=5, adjust=False).mean()
            
            # RSI
            delta = df['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=5).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=5).mean()
            rs = gain / loss
            df['rsi'] = 100 - (100 / (1 + rs))
            
            # MACD
            exp1 = df['close'].ewm(span=5, adjust=False).mean()
            exp2 = df['close'].ewm(span=10, adjust=False).mean()
            df['macd'] = exp1 - exp2
            df['macd_signal'] = df['macd'].ewm(span=3, adjust=False).mean()
            df['macd_hist'] = df['macd'] - df['macd_signal']
            
            # Bollinger Bands
            df['bb_middle'] = df['close'].rolling(window=5).mean()
            bb_std = df['close'].rolling(window=5).std()
            df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
            df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
            
            # ATR
            high_low = df['high'] - df['low']
            high_close = np.abs(df['high'] - df['close'].shift())
            low_close = np.abs(df['low'] - df['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = np.max(ranges, axis=1)
            df['atr'] = true_range.rolling(5).mean()
            
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return None
        
        # Get the latest values
        result = df.iloc[-1].to_dict()
        result.update({
            'symbol': symbol,
            'timestamp': datetime.utcnow().isoformat(),
            'source': 'technical_analysis'
        })
        
        logger.info(f"Successfully calculated indicators for {symbol}")
        return result
        
    except Exception as e:
        logger.error(f"Error calculating technical indicators: {e}")
        logger.error(f"Input data: {data}")
        return None

def main():
    try:
        # Create Kafka consumer and producer
        consumer = create_kafka_consumer()
        producer = create_kafka_producer()
        
        logger.info("Starting Technical Analysis job...")
        
        # Process messages
        for message in consumer:
            try:
                market_data = message.value
                result = calculate_technical_indicators(market_data)
                
                if result:
                    producer.send('crypto-analysis-data', value=result)
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
        logger.info("Technical Analysis job completed")

if __name__ == "__main__":
    main() 