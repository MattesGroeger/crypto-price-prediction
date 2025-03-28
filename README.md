# Crypto Analysis Platform

A real-time cryptocurrency analysis platform that combines market data, social sentiment, and technical analysis to provide price predictions.

## Features

- Real-time market data collection from Binance and CoinGecko
- Social sentiment analysis from Reddit
- Technical analysis with multiple indicators
- Price prediction using LSTM neural networks
- Real-time data processing with Kafka
- Containerized deployment with Docker

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Binance API key and secret
- Reddit API credentials (client ID and secret)

## Project Structure

```
.
├── src/
│   ├── collectors/           # Data collection modules
│   │   ├── binance_collector.py
│   │   ├── reddit_collector.py
│   │   └── coingecko_collector.py
│   ├── flink_jobs/          # Data processing jobs
│   │   ├── sentiment_analysis.py
│   │   ├── technical_analysis.py
│   │   └── price_prediction.py
│   ├── producers/           # Kafka producers
│   │   └── kafka_producer.py
│   └── main.py             # Main application entry point
├── docker/                 # Docker configuration files
├── .env.example           # Example environment variables
├── requirements.txt       # Python dependencies
└── docker-compose.yml     # Docker Compose configuration
```

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd crypto-app
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file based on `.env.example`:
```bash
cp .env.example .env
```

5. Add your API credentials to the `.env` file:
```
BINANCE_API_KEY=your_binance_api_key
BINANCE_API_SECRET=your_binance_api_secret
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USER_AGENT=your_user_agent
```

## Running the Application

1. Start the required services (Kafka, etc.):
```bash
make run
```

2. Start the data collectors:
```bash
python src/main.py
```

3. Start the sentiment analysis job:
```bash
python src/flink_jobs/sentiment_analysis.py
```

4. Start the technical analysis job:
```bash
python src/flink_jobs/technical_analysis.py
```

5. Start the price prediction job:
```bash
python src/flink_jobs/price_prediction.py
```

## Data Flow

1. **Data Collection**:
   - Market data from Binance (1-minute intervals)
   - Market data from CoinGecko (1-minute intervals)
   - Social data from Reddit (posts and comments)

2. **Sentiment Analysis**:
   - Analyzes Reddit posts and comments
   - Calculates sentiment scores (-1 to 1)
   - Processes posts from multiple subreddits
   - Real-time sentiment updates

3. **Technical Analysis**:
   - Calculates various technical indicators:
     - SMA (Simple Moving Average)
     - EMA (Exponential Moving Average)
     - RSI (Relative Strength Index)
     - MACD (Moving Average Convergence Divergence)
     - Bollinger Bands
     - ATR (Average True Range)

4. **Price Prediction**:
   - Uses LSTM neural network
   - Requires 60 data points for initial prediction
   - Predicts price for the next hour
   - Reinforcement Learning mode:
     - Learns through trading interactions
     - Adapts to market conditions
     - Makes trading decisions
     - Optimizes for profit/loss
     - Requires longer training period

## Kafka Topics

- `crypto-reddit-data`: Raw Reddit posts and comments
- `crypto-sentiment-data`: Analyzed sentiment from social data
- `crypto-binance-data`: Market data from Binance
- `crypto-coingecko-data`: Market data from CoinGecko
- `crypto-analysis-data`: Technical analysis results
- `crypto-prediction-data`: Price predictions

## Data Sources

1. **Binance Data**:
   - Real-time price data
   - Trading volume
   - Order book updates
   - 1-minute intervals

2. **CoinGecko Data**:
   - Current prices
   - 24-hour price changes
   - Market capitalization
   - 1-minute intervals

3. **Reddit Data**:
   - Posts from r/Bitcoin, r/CryptoCurrency, r/CryptoMarkets
   - Post titles and content
   - Sentiment analysis scores

## Monitoring

- Check the logs of each component for real-time monitoring
- The technical analysis job shows calculated indicators
- The price prediction job shows collected data points and predictions

## Stopping the Application

```bash
make stop
```

## Notes

- The price prediction model requires 60 data points (1 hour of data) before making predictions
- Technical analysis is performed on 5-minute windows
- Sentiment analysis is performed on all new Reddit posts
- All data is processed in real-time using Kafka streams
- Reinforcement Learning mode:
  - Requires significant historical data for training
  - Adapts to changing market conditions
  - Can implement trading strategies
  - Higher computational requirements

## Troubleshooting

1. If Kafka connection fails:
   - Check if Kafka container is running: `docker ps`
   - Verify Kafka is accessible: `nc -zv localhost 9092`

2. If data collection fails:
   - Verify API credentials in `.env`
   - Check API rate limits
   - Verify network connectivity

3. If predictions are not showing:
   - Wait for 60 data points to be collected
   - Check technical analysis logs for successful calculations
   - Verify data flow through Kafka topics 