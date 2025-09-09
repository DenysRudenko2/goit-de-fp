# Olympic Data Pipeline - Makefile
# =================================
# Commands for managing the data pipeline infrastructure

.PHONY: help start stop restart start-streaming fix-permissions clean status logs

# Default target - show help
help:
	@echo "Olympic Data Pipeline - Available Commands"
	@echo "=========================================="
	@echo "  make start           - Start batch processing infrastructure"
	@echo "  make start-streaming - Start streaming infrastructure (Kafka + Airflow + Spark)"
	@echo "  make stop            - Stop all containers and clean up"
	@echo "  make restart         - Restart all services"
	@echo "  make fix-permissions - Fix volume permissions for data lake"
	@echo "  make status          - Show status of all containers"
	@echo "  make logs            - Show logs from all containers"
	@echo "  make clean           - Remove all data and reset environment"
	@echo ""
	@echo "Batch Pipeline Commands:"
	@echo "  make trigger-dag     - Trigger the batch data pipeline DAG"
	@echo "  make show-data       - Display sample data from each layer"
	@echo ""
	@echo "Streaming Pipeline Commands:"
	@echo "  make create-topics   - Create Kafka topics"
	@echo "  make mysql-to-kafka  - Load MySQL data to Kafka"
	@echo "  make stream-process  - Run streaming processor"
	@echo "  make show-kafka-data - Display Kafka messages in table format"
	@echo "  make test-kafka      - Test Kafka connectivity"

# Start infrastructure (same as streaming since we use unified setup)
start:
	@echo "Starting data processing infrastructure..."
	docker compose -f docker-compose-streaming.yaml up -d
	@echo "Waiting for services to be ready..."
	@sleep 15
	@echo "Infrastructure is ready!"
	@echo "Access points:"
	@echo "  - Airflow UI: http://localhost:8090 (user: airflow, pass: airflow)"
	@echo "  - Spark UI: http://localhost:8080"

# Start streaming infrastructure with Kafka, Airflow, and Spark
start-streaming:
	@echo "Starting streaming infrastructure..."
	docker compose -f docker-compose-streaming.yaml up -d
	@echo "Waiting for services to initialize..."
	@sleep 15
	@echo "Streaming infrastructure is ready!"
	@echo "Access points:"
	@echo "  - Airflow UI: http://localhost:8090 (user: airflow, pass: airflow)"
	@echo "  - Spark UI: http://localhost:8080"

# Stop all containers
stop:
	@echo "Stopping all containers..."
	docker compose -f docker-compose-streaming.yaml down
	@echo "All services stopped."

# Restart all services
restart: stop start-streaming

# Fix permissions for data lake volumes
fix-permissions:
	@echo "Fixing permissions for data lake volumes..."
	docker compose -f docker-compose-streaming.yaml exec airflow-webserver chmod -R 777 /tmp/data_lake
	docker compose -f docker-compose-streaming.yaml exec spark-worker chmod -R 777 /tmp/data_lake
	@echo "Permissions fixed."

# Show status of all containers
status:
	@echo "Container Status:"
	@echo "================="
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Show logs from all containers
logs:
	docker compose -f docker-compose-streaming.yaml logs --tail=50

# Clean all data and volumes
clean:
	@echo "WARNING: This will remove all data and volumes!"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo "Cleaning up..."
	docker compose -f docker-compose-streaming.yaml down -v
	@echo "Cleanup complete."

# Trigger the data pipeline DAG
trigger-dag:
	@echo "Triggering data pipeline DAG..."
	docker compose -f docker-compose-streaming.yaml exec airflow-webserver \
		airflow dags trigger rudenko_data_lake_pipeline
	@echo "DAG triggered. Check Airflow UI for progress."

# ========== STREAMING PIPELINE COMMANDS ==========

# Create Kafka topics
create-topics:
	@echo "Creating Kafka topics..."
	docker compose -f docker-compose-streaming.yaml exec airflow-webserver \
		python /opt/airflow/01_create_kafka_topics.py
	@echo "Kafka topics created successfully."

# Load MySQL data to Kafka
mysql-to-kafka:
	@echo "Loading MySQL data to Kafka..."
	docker compose -f docker-compose-streaming.yaml exec airflow-webserver \
		python /opt/airflow/02_mysql_to_kafka.py
	@echo "Data loaded to Kafka successfully."

# Run streaming processor
stream-process:
	@echo "Starting streaming processor..."
	@echo "This will run continuously. Press Ctrl+C to stop."
	docker compose -f docker-compose-streaming.yaml exec airflow-webserver \
		python /opt/airflow/03_streaming_processor_fixed.py

# Display Kafka messages in table format
show-kafka-data:
	@echo "Displaying Kafka messages in table format..."
	docker compose -f docker-compose-streaming.yaml exec airflow-webserver \
		python /opt/airflow/05_kafka_table_display.py

# Test Kafka connectivity (alias for create-topics)
test-kafka: create-topics

# ========== BATCH PIPELINE COMMANDS ==========

# Show sample data from each layer
show-data:
	@echo "Checking data lake contents..."
	@echo "Bronze layer:"
	@docker compose -f docker-compose-streaming.yaml exec airflow-webserver \
		find /tmp/data_lake/bronze -name "*.parquet" 2>/dev/null | head -5
	@echo ""
	@echo "Silver layer:"
	@docker compose -f docker-compose-streaming.yaml exec airflow-webserver \
		find /tmp/data_lake/silver -name "*.parquet" 2>/dev/null | head -5
	@echo ""
	@echo "Gold layer:"
	@docker compose -f docker-compose-streaming.yaml exec airflow-webserver \
		find /tmp/data_lake/gold -name "*.parquet" 2>/dev/null | head -5