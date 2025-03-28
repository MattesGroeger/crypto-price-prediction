import json
import logging
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple
from kafka import KafkaConsumer, KafkaProducer
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TradingEnvironment:
    def __init__(self, initial_balance: float = 10000.0):
        self.initial_balance = initial_balance
        self.balances: Dict[str, float] = {}  # Separate balance for each symbol
        self.positions: Dict[str, float] = {}  # Separate position for each symbol
        self.entry_prices: Dict[str, float] = {}  # Entry price for each position
        self.trades: Dict[str, List[Dict]] = {}  # Trade history for each symbol
        
    def reset(self, symbol: str):
        """Reset environment for a specific symbol"""
        self.balances[symbol] = self.initial_balance
        self.positions[symbol] = 0.0
        self.entry_prices[symbol] = 0.0
        self.trades[symbol] = []
        
    def _get_state(self) -> np.ndarray:
        """Get current environment state"""
        # Calculate total portfolio value across all symbols
        total_value = sum(self.balances.values()) if self.balances else self.initial_balance
        total_positions = sum(self.positions.values()) if self.positions else 0.0
        
        return np.array([
            total_value / self.initial_balance,  # Normalized portfolio value
            total_positions / self.initial_balance,  # Normalized total positions
            len(self.positions) / 3.0,  # Number of active positions (normalized)
            sum(1 for p in self.positions.values() if p > 0) / 3.0  # Number of long positions (normalized)
        ])
        
    def step(self, action: int, price: float, symbol: str) -> Tuple[float, bool]:
        """Execute a trading action"""
        if symbol not in self.balances:
            self.reset(symbol)
            
        reward = 0.0
        done = False
        
        # Calculate position size (max 10% of balance)
        max_position = self.balances[symbol] * 0.1
        current_position = self.positions[symbol]
        
        if action == 0:  # Sell
            if current_position > 0:
                # Calculate PnL
                pnl = (price - self.entry_prices[symbol]) * current_position
                reward = pnl / self.initial_balance  # Normalize reward
                
                # Update balance and close position
                self.balances[symbol] += pnl
                self.positions[symbol] = 0.0
                self.entry_prices[symbol] = 0.0
                
                # Record trade
                self.trades[symbol].append({
                    'action': 'sell',
                    'price': price,
                    'pnl': pnl,
                    'balance': self.balances[symbol]
                })
                
        elif action == 2:  # Buy
            if current_position == 0:
                # Calculate position size
                position_size = min(max_position, self.balances[symbol] * 0.1)
                
                # Update position and balance
                self.positions[symbol] = position_size / price
                self.balances[symbol] -= position_size
                self.entry_prices[symbol] = price
                
                # Record trade
                self.trades[symbol].append({
                    'action': 'buy',
                    'price': price,
                    'size': position_size,
                    'balance': self.balances[symbol]
                })
                
        # Calculate reward based on position value
        if current_position > 0:
            position_value = current_position * price
            reward = (position_value - (current_position * self.entry_prices[symbol])) / self.initial_balance
            
        return reward, done

class RLPricePredictionModel:
    def __init__(self, window_size: int = 5):
        self.window_size = window_size
        # Initialize hyperparameters first
        self.batch_size = 32
        self.gamma = 0.95  # Discount factor
        self.epsilon = 1.0  # Exploration rate
        self.epsilon_min = 0.01
        self.epsilon_decay = 0.995
        self.learning_rate = 0.001
        
        # Initialize data storage
        self.data_history: Dict[str, List[Dict]] = {}
        self.training_data: List[Tuple[np.ndarray, int, float, np.ndarray, bool]] = []
        
        # Initialize environment and model
        self.environment = TradingEnvironment()
        self.model = self._build_model()
        
        # Initialize logging
        self.step_count = 0
        self.total_reward = 0.0
        self.win_rate = 0.0
        self.total_trades = 0
        self.winning_trades = 0
        
        logger.info(f"Initialized model with window_size={window_size}")
        
    def _build_model(self) -> Sequential:
        model = Sequential([
            LSTM(50, return_sequences=True, input_shape=(self.window_size, 10)),
            Dropout(0.2),
            LSTM(50, return_sequences=False),
            Dropout(0.2),
            Dense(3, activation='linear')  # 3 actions: sell, hold, buy
        ])
        model.compile(optimizer=Adam(learning_rate=self.learning_rate), loss='mse')
        return model
    
    def _prepare_state(self, data: List[Dict]) -> np.ndarray:
        """Prepare state for the model"""
        if len(data) < self.window_size:
            logger.info(f"Not enough data points: {len(data)}/{self.window_size}")
            return None
            
        state = []
        for i, point in enumerate(data[-self.window_size:]):
            # Calculate price changes
            if i > 0:
                price_change = (point['close'] - data[-self.window_size+i-1]['close']) / data[-self.window_size+i-1]['close']
                volume_change = (point['volume'] - data[-self.window_size+i-1]['volume']) / data[-self.window_size+i-1]['volume']
            else:
                price_change = 0.0
                volume_change = 0.0
            
            state.append([
                point['open'],
                point['high'],
                point['low'],
                point['close'],
                point['volume'],
                point['rsi'] / 100.0,  # Normalize RSI
                point['macd'] / 1000.0,  # Normalize MACD
                point['macd_hist'] / 100.0,  # Normalize MACD histogram
                price_change,  # Add price change
                volume_change  # Add volume change
            ])
        
        state_array = np.array(state)
        logger.info(f"Prepared state shape: {state_array.shape}")
        return state_array
    
    def predict(self, symbol: str, data: List[Dict]) -> Tuple[float, Dict]:
        """Make a prediction and trading decision"""
        if symbol not in self.data_history:
            self.data_history[symbol] = []
            logger.info(f"Initialized data history for {symbol}")
            
        self.data_history[symbol].append(data[-1])
        if len(self.data_history[symbol]) > self.window_size:
            self.data_history[symbol] = self.data_history[symbol][-self.window_size:]
            
        if len(self.data_history[symbol]) < self.window_size:
            logger.info(f"Collecting data for {symbol}: {len(self.data_history[symbol])}/{self.window_size} points")
            return data[-1]['close'], {
                'action': 'hold',
                'confidence': 0.0,
                'balance': self.environment.balances[symbol],
                'position': self.environment.positions[symbol],
                'data_points': len(self.data_history[symbol])
            }
            
        state = self._prepare_state(self.data_history[symbol])
        if state is None:
            logger.warning(f"Failed to prepare state for {symbol}")
            return data[-1]['close'], {
                'action': 'hold',
                'confidence': 0.0,
                'balance': self.environment.balances[symbol],
                'position': self.environment.positions[symbol],
                'data_points': len(self.data_history[symbol])
            }
            
        # Get current environment state
        env_state = self.environment._get_state()
        logger.info(f"Environment state shape: {env_state.shape}")
        
        # Epsilon-greedy action selection
        if np.random.random() < self.epsilon:
            action = np.random.randint(0, 3)
            q_values = np.zeros(3)  # Default Q-values for random action
            logger.info(f"Random action for {symbol}: {action}")
        else:
            try:
                q_values = self.model.predict(state.reshape(1, self.window_size, 10))[0]
                action = np.argmax(q_values)
                logger.info(f"Learned action for {symbol}: {action}, Q-values: {q_values}")
            except Exception as e:
                logger.error(f"Error making prediction: {e}")
                logger.error(f"State shape: {state.shape}")
                action = 1  # Default to hold on error
                q_values = np.zeros(3)
        
        # Execute action in environment
        reward, done = self.environment.step(action, data[-1]['close'], symbol)
        
        # Update trade statistics
        if len(self.environment.trades[symbol]) > self.total_trades:
            self.total_trades += 1
            if reward > 0:
                self.winning_trades += 1
            self.win_rate = self.winning_trades / self.total_trades
        
        # Store experience for training (only market state)
        self.training_data.append((state, action, reward, state, done))
        self.total_reward += reward
        self.step_count += 1
        
        # Train on batch if enough data
        if len(self.training_data) >= self.batch_size:
            self._train()
            
        # Decay epsilon
        if self.epsilon > self.epsilon_min:
            self.epsilon *= self.epsilon_decay
            
        # Map action to trading decision
        action_map = {0: 'sell', 1: 'hold', 2: 'buy'}
        
        # Log performance metrics
        if self.step_count % 100 == 0:
            logger.info(f"Performance metrics - Steps: {self.step_count}, Avg Reward: {self.total_reward/self.step_count:.4f}, "
                       f"Epsilon: {self.epsilon:.4f}, Win Rate: {self.win_rate:.2%}")
        
        return data[-1]['close'], {
            'action': action_map[action],
            'confidence': float(np.max(q_values)),
            'balance': self.environment.balances[symbol],
            'position': self.environment.positions[symbol],
            'reward': float(reward),
            'epsilon': float(self.epsilon),
            'win_rate': float(self.win_rate)
        }
    
    def _train(self):
        """Train the model on a batch of experiences"""
        if len(self.training_data) < self.batch_size:
            return
            
        batch = np.random.choice(len(self.training_data), self.batch_size, replace=False)
        states = []
        targets = []
        
        for idx in batch:
            state, action, reward, next_state, done = self.training_data[idx]
            
            try:
                # Get current Q-values
                current_q = self.model.predict(state.reshape(1, self.window_size, 10))[0]
                
                # Calculate target Q-value
                if done:
                    target_q = reward
                else:
                    # For next state, we only need the market state part
                    next_market_state = next_state[:self.window_size * 10].reshape(1, self.window_size, 10)
                    next_q = self.model.predict(next_market_state)[0]
                    target_q = reward + self.gamma * np.max(next_q)
                    
                # Update Q-value for the action taken
                current_q[action] = target_q
                states.append(state)  # Use the full market state
                targets.append(current_q)
                
            except Exception as e:
                logger.error(f"Error in training batch: {e}")
                logger.error(f"State shape: {state.shape}")
                continue
            
        if not states:  # If all states failed
            return
            
        # Train the model
        try:
            history = self.model.fit(
                np.array(states).reshape(self.batch_size, self.window_size, 10),
                np.array(targets),
                epochs=1,
                verbose=0
            )
            
            # Log training metrics
            logger.info(f"Training loss: {history.history['loss'][0]:.4f}")
            
        except Exception as e:
            logger.error(f"Error during model training: {e}")
            return
        
        # Clear old experiences
        self.training_data = self.training_data[-1000:]

def main():
    # Initialize Kafka consumer and producer
    consumer = KafkaConsumer(
        'crypto-analysis-data',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    # Initialize model
    model = RLPricePredictionModel()
    
    try:
        for message in consumer:
            try:
                data = message.value
                symbol = data['symbol']
                
                # Make prediction and get trading decision
                current_price, prediction = model.predict(symbol, [data])
                
                # Prepare output message
                output = {
                    'symbol': symbol,
                    'timestamp': datetime.utcnow().isoformat(),
                    'current_price': current_price,
                    'predicted_price': current_price,  # We're now focusing on actions rather than price prediction
                    'action': prediction['action'],
                    'confidence': prediction['confidence'],
                    'balance': prediction['balance'],
                    'position': prediction['position'],
                    'source': 'price_prediction'
                }
                
                # Send prediction to Kafka
                producer.send('crypto-prediction-data', output)
                logger.info(f"Trading decision for {symbol}: {prediction['action']} (confidence: {prediction['confidence']:.2f}, balance: {prediction['balance']:.2f})")
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
            
    except KeyboardInterrupt:
        logger.info("Stopping price prediction job...")
    finally:
        consumer.close()
        producer.close()

if __name__ == "__main__":
    main() 