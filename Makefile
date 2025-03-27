VENV_DIR := .venv
PYTHON := $(VENV_DIR)/bin/python
PYTHONPATH := $(shell pwd)

# Create virtual environment and install dependencies
setup:
	python3.9 -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install -U pip
	$(VENV_DIR)/bin/pip install -r requirements.txt
	echo "Virtual environment created. Activate it with: source $(VENV_DIR)/bin/activate"

# Clean up the environment
clean:
	rm -rf $(VENV_DIR)
	rm -rf __pycache__
	rm -rf src/__pycache__
	rm -rf src/*/__pycache__

.PHONY: setup clean start stop kafka-topics flink-jobs run

start:
	docker-compose up -d

stop:
	docker-compose down

kafka-topics:
	sleep 10
	docker-compose exec kafka /usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic crypto-reddit-data --partitions 1 --replication-factor 1
	docker-compose exec kafka /usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic crypto-binance-data --partitions 1 --replication-factor 1
	docker-compose exec kafka /usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic crypto-sentiment-data --partitions 1 --replication-factor 1
	docker-compose exec kafka /usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic crypto-analysis-data --partitions 1 --replication-factor 1
	docker-compose exec kafka /usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic crypto-prediction-data --partitions 1 --replication-factor 1

flink-jobs:
	$(PYTHON) src/flink_jobs/sentiment_analysis.py &
	$(PYTHON) src/flink_jobs/technical_analysis.py &
	$(PYTHON) src/flink_jobs/price_prediction.py &

run: start kafka-topics flink-jobs
