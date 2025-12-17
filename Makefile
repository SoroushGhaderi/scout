# Scout - Makefile
# Convenience commands for Docker operations

# Docker Compose file location
COMPOSE_FILE = -f docker/docker-compose.yml

.PHONY: help build up down restart logs shell clean init-dirs

help: ## Show this help message
	@echo "Scout - Docker Commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: ## Build Docker images
	docker-compose $(COMPOSE_FILE) build

up: ## Start all services
	docker-compose $(COMPOSE_FILE) up -d

down: ## Stop all services
	docker-compose $(COMPOSE_FILE) down

restart: ## Restart all services
	docker-compose $(COMPOSE_FILE) restart

logs: ## Show logs from all services
	docker-compose $(COMPOSE_FILE) logs -f

logs-scraper: ## Show scraper logs
	docker-compose $(COMPOSE_FILE) logs -f scraper

logs-clickhouse: ## Show ClickHouse logs
	docker-compose $(COMPOSE_FILE) logs -f clickhouse

shell: ## Open shell in scraper container
	docker-compose $(COMPOSE_FILE) exec scraper bash

init-dirs: ## Initialize data and logs directories (optional, auto-created on first run)
	docker-compose $(COMPOSE_FILE) exec scraper python scripts/ensure_directories.py

clickhouse-client: ## Open ClickHouse client
	docker-compose $(COMPOSE_FILE) exec clickhouse clickhouse-client

rebuild: ## Rebuild and restart services
	docker-compose $(COMPOSE_FILE) up -d --build

clean: ## Remove containers and volumes (WARNING: Deletes data)
	docker-compose $(COMPOSE_FILE) down -v

clean-all: ## Remove containers, volumes, and images
	docker-compose $(COMPOSE_FILE) down -v --rmi all

# Scraping commands
scrape-fotmob: ## Scrape FotMob data (usage: make scrape-fotmob DATE=20251113)
	docker-compose $(COMPOSE_FILE) exec scraper python scripts/scrape_fotmob.py $(DATE)

scrape-aiscore: ## Scrape AIScore data (usage: make scrape-aiscore DATE=20251113)
	docker-compose $(COMPOSE_FILE) exec scraper python scripts/scrape_aiscore.py $(DATE)

load-fotmob: ## Load FotMob data to ClickHouse (usage: make load-fotmob DATE=20251113)
	docker-compose $(COMPOSE_FILE) exec scraper python scripts/load_clickhouse.py --scraper fotmob --date $(DATE)

load-aiscore: ## Load AIScore data to ClickHouse (usage: make load-aiscore DATE=20251113)
	docker-compose $(COMPOSE_FILE) exec scraper python scripts/load_clickhouse.py --scraper aiscore --date $(DATE)

# ClickHouse optimization
optimize-tables: ## Optimize and deduplicate all ClickHouse tables
	@echo "Optimizing ClickHouse tables..."
	docker-compose $(COMPOSE_FILE) exec -T clickhouse clickhouse-client --user fotmob_user --password fotmob_pass < clickhouse/init/03_optimize_tables.sql
	@echo "Table optimization complete!"

# Status checks
status: ## Show status of all services
	docker-compose $(COMPOSE_FILE) ps

health: ## Check ClickHouse health
	docker-compose $(COMPOSE_FILE) exec clickhouse clickhouse-client --query "SELECT 1"

# Development
dev: ## Start in development mode with override
	docker-compose $(COMPOSE_FILE) -f docker/docker-compose.override.yml up -d

test: ## Run tests in container
	docker-compose $(COMPOSE_FILE) exec scraper pytest

