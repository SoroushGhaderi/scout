# Scout - Makefile
# Convenience commands for Docker operations

.PHONY: help build up down restart logs shell clean

help: ## Show this help message
	@echo "Scout - Docker Commands"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: ## Build Docker images
	docker-compose build

up: ## Start all services
	docker-compose up -d

down: ## Stop all services
	docker-compose down

restart: ## Restart all services
	docker-compose restart

logs: ## Show logs from all services
	docker-compose logs -f

logs-scraper: ## Show scraper logs
	docker-compose logs -f scraper

logs-clickhouse: ## Show ClickHouse logs
	docker-compose logs -f clickhouse

shell: ## Open shell in scraper container
	docker-compose exec scraper bash

clickhouse-client: ## Open ClickHouse client
	docker-compose exec clickhouse clickhouse-client

rebuild: ## Rebuild and restart services
	docker-compose up -d --build

clean: ## Remove containers and volumes (WARNING: Deletes data)
	docker-compose down -v

clean-all: ## Remove containers, volumes, and images
	docker-compose down -v --rmi all

# Scraping commands
scrape-fotmob: ## Scrape FotMob data (usage: make scrape-fotmob DATE=20251113)
	docker-compose exec scraper python scripts/scrape_fotmob.py $(DATE)

scrape-aiscore: ## Scrape AIScore data (usage: make scrape-aiscore DATE=20251113)
	docker-compose exec scraper python scripts/scrape_aiscore.py $(DATE)

load-fotmob: ## Load FotMob data to ClickHouse (usage: make load-fotmob DATE=20251113)
	docker-compose exec scraper python scripts/load_clickhouse.py --scraper fotmob --date $(DATE)

load-aiscore: ## Load AIScore data to ClickHouse (usage: make load-aiscore DATE=20251113)
	docker-compose exec scraper python scripts/load_clickhouse.py --scraper aiscore --date $(DATE)

# ClickHouse optimization
optimize-tables: ## Optimize and deduplicate all ClickHouse tables
	@echo "Optimizing ClickHouse tables..."
	docker-compose exec -T clickhouse clickhouse-client --user fotmob_user --password fotmob_pass < clickhouse/init/03_optimize_tables.sql
	@echo "Table optimization complete!"

# Status checks
status: ## Show status of all services
	docker-compose ps

health: ## Check ClickHouse health
	docker-compose exec clickhouse clickhouse-client --query "SELECT 1"

# Development
dev: ## Start in development mode with override
	docker-compose -f docker-compose.yml -f docker-compose.override.yml up -d

test: ## Run tests in container
	docker-compose exec scraper pytest

