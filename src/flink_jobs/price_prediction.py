from kafka import KafkaConsumer, KafkaProducer
import json
import numpy as np
import pandas as pd
import tensorflow as tf
import logging
from datetime import datetime, timedelta
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PricePredictionModel:
    def __init__(self, window_size=60):
        """Initialize the model"""
        self.window_size = window_size
        self.model = self._build_model()
        self.scaler = MinMaxScaler()
        
    def _build_model(self):
        """Build the LSTM model"""
        model = Sequential([
            LSTM(50, return_sequences=True, input_shape=(self.window_size, 1)),
            LSTM(50, return_sequences=False),
            Dense(25),
            Dense(1)
        ])
        model.compile(optimizer='adam', loss='mse')
        return model
        
    def prepare_data(self, data):
        """Prepare data for prediction"""
        try:
            # Extract close prices
            prices = [float(d['close']) for d in data]
            
            # Scale the data
            scaled_data = self.scaler.fit_transform(np.array(prices).reshape(-1, 1))
            
            # Create sequences
            X = []
            for i in range(len(scaled_data) - self.window_size):
                X.append(scaled_data[i:(i + self.window_size)])
                
            return np.array(X)
            
        except Exception as e:
            logger.error(f"Error preparing data: {e}")
            return None
            
    def predict(self, data):
        """Make price predictions"""
        try:
            X = self.prepare_data(data)
            if X is None or len(X) == 0:
                return None
                
            # Make prediction
            prediction = self.model.predict(X[-1:])
            
            # Inverse transform
            prediction = self.scaler.inverse_transform(prediction)[0][0]
            
            return float(prediction)
            
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            return None

def create_kafka_consumer():
    """Create Kafka consumer"""
    return KafkaConsumer(
        'crypto-analysis-data',
        bootstrap_servers='localhost:9092',
        group_id='price_prediction_group',
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def create_kafka_producer():
    """Create Kafka producer"""
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def main():
    try:
        # Create Kafka consumer and producer
        consumer = create_kafka_consumer()
        producer = create_kafka_producer()
        
        # Initialize model
        model = PricePredictionModel(window_size=60)
        
        # Initialize data storage
        data_history = {}
        
        logger.info("Starting Price Prediction job...")
        
        # Process messages
        for message in consumer:
            try:
                market_data = message.value
                logger.info(f"Received market data: {market_data}")
                
                symbol = market_data['symbol']
                
                # Initialize or update data history for the symbol
                if symbol not in data_history:
                    data_history[symbol] = []
                    logger.info(f"Initialized data history for {symbol}")
                
                # Add new data point to history
                data_history[symbol].append(market_data)
                logger.info(f"Added data point for {symbol}. Total points: {len(data_history[symbol])}")
                
                # Keep only the last 100 data points
                if len(data_history[symbol]) > 100:
                    data_history[symbol] = data_history[symbol][-100:]
                    logger.info(f"Trimmed data history for {symbol} to 100 points")
                
                # Make prediction if we have enough data
                if len(data_history[symbol]) >= model.window_size:
                    logger.info(f"Making prediction for {symbol} with {len(data_history[symbol])} data points")
                    prediction = model.predict(data_history[symbol])
                    
                    if prediction is not None:
                        # Calculate prediction timestamp (1 hour ahead)
                        prediction_time = datetime.fromisoformat(market_data['timestamp']) + timedelta(hours=1)
                        
                        result = {
                            'symbol': symbol,
                            'timestamp': datetime.utcnow().isoformat(),
                            'prediction_time': prediction_time.isoformat(),
                            'current_price': float(market_data['close']),
                            'predicted_price': prediction,
                            'source': 'price_prediction'
                        }
                        
                        producer.send('crypto-prediction-data', value=result)
                        producer.flush()
                        
                        logger.info(f"Predicted price for {symbol}: Current: {result['current_price']:.2f}, Predicted: {prediction:.2f}")
                    else:
                        logger.warning(f"Failed to make prediction for {symbol}")
                else:
                    logger.info(f"Not enough data points for {symbol}. Need {model.window_size}, got {len(data_history[symbol])}")
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                logger.error(f"Message content: {message.value}")
                continue
                
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        consumer.close()
        producer.close()
        logger.info("Price Prediction job completed")

if __name__ == "__main__":
    main() 