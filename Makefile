.PHONY: help install test clean start status sync setup

help: ## Show this help message
	@echo "DataProxy - Database proxy with local write caching"
	@echo ""
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt

install-dev: ## Install development dependencies
	pip install -r requirements.txt
	pip install pytest pytest-cov black flake8

test: ## Run tests
	python -m pytest tests/ -v

test-cov: ## Run tests with coverage
	python -m pytest tests/ --cov=dataproxy --cov-report=html

lint: ## Run linting
	black dataproxy/ tests/
	flake8 dataproxy/ tests/

clean: ## Clean up generated files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf build/
	rm -rf dist/

start: ## Start the DataProxy server
	python -m dataproxy.main start

status: ## Show DataProxy status
	python -m dataproxy.main status

sync: ## Synchronize table schemas
	python -m dataproxy.main sync

sync-table: ## Synchronize specific table (usage: make sync-table TABLE=users)
	python -m dataproxy.main sync --table $(TABLE)

test-connectivity: ## Test database connectivity
	python -m dataproxy.main test

setup: ## Setup local database
	python scripts/setup_local_db.py

setup-env: ## Copy environment file
	cp env.example .env
	@echo "Please edit .env with your database configuration"

build: ## Build package
	python setup.py sdist bdist_wheel

install-local: ## Install package locally
	pip install -e .

uninstall: ## Uninstall package
	pip uninstall dataproxy -y

docker-build: ## Build Docker image
	docker build -t dataproxy .

docker-run: ## Run DataProxy in Docker
	docker run -p 3307:3307 --env-file .env dataproxy

docker-stop: ## Stop running Docker containers
	docker stop $$(docker ps -q --filter ancestor=dataproxy)

logs: ## Show DataProxy logs
	tail -f dataproxy.log

reset-local: ## Reset local database (WARNING: This will delete all local data)
	@echo "WARNING: This will delete all local data!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		echo "Dropping local database..."; \
		python -c "from dataproxy.config import Config; import pymysql; conn = pymysql.connect(host=Config.LOCAL_DB_HOST, port=Config.LOCAL_DB_PORT, user=Config.LOCAL_DB_USER, password=Config.LOCAL_DB_PASSWORD); conn.cursor().execute(f'DROP DATABASE IF EXISTS {Config.LOCAL_DB_NAME}'); conn.close(); print('Local database dropped')"; \
	else \
		echo "Operation cancelled"; \
	fi

check-deps: ## Check if all dependencies are installed
	@echo "Checking dependencies..."
	@python -c "import pymysql, sqlalchemy, click, rich, dotenv; print('✅ All dependencies installed')" || echo "❌ Missing dependencies - run 'make install'"

format: ## Format code with black
	black dataproxy/ tests/ scripts/

check-format: ## Check code formatting
	black --check dataproxy/ tests/ scripts/

dev-setup: ## Complete development setup
	@echo "Setting up development environment..."
	make install-dev
	make setup-env
	make setup
	@echo "Development setup complete! Edit .env with your database settings."

quick-start: ## Quick start with default settings
	@echo "Starting DataProxy with default settings..."
	@echo "Make sure you have MySQL running and have configured .env"
	make setup
	make start
