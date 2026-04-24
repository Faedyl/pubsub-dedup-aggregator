.PHONY: help build run test docker-build docker-run clean format lint

help:
	@echo "UTS Pub-Sub Log Aggregator - Available Commands"
	@echo ""
	@echo "Development:"
	@echo "  make test              - Run unit tests"
	@echo "  make test-cov          - Run tests with coverage"
	@echo "  make lint              - Run linter (flake8)"
	@echo "  make format            - Format code (black)"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build      - Build Docker image"
	@echo "  make docker-run        - Run container"
	@echo "  make docker-compose    - Run with docker-compose"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean             - Clean temporary files"
	@echo "  make install           - Install dependencies"

install:
	@echo "Installing dependencies..."
	pip install -r requirements.txt

test:
	@echo "Running tests..."
	pytest tests/ -v

test-cov:
	@echo "Running tests with coverage..."
	pytest tests/ -v --cov=src --cov-report=html

lint:
	@echo "Running linter..."
	flake8 src/ tests/ --max-line-length=100

format:
	@echo "Formatting code..."
	black src/ tests/

docker-build:
	@echo "Building Docker image..."
	docker build -t uts-aggregator .
	@echo "Build successful: uts-aggregator"

docker-run:
	@echo "Running Docker container..."
	docker run -p 8080:8080 -v aggregator_data:/app/data uts-aggregator

docker-compose:
	@echo "Starting with docker-compose..."
	docker-compose up -d

clean:
	@echo "Cleaning temporary files..."
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache .coverage htmlcov build dist *.egg-info
	rm -f *.db *.sqlite *.sqlite3

.PHONY: help install test test-cov lint format docker-build docker-run docker-compose clean
