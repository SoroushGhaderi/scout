# Scout - Sports Data Pipeline

> Production-ready football data scraper with FotMob API and AIScore web scraping, integrated with ClickHouse data warehouse.

## Quick Start

```bash
# 1. Setup
git clone <repository-url>
cd scout
cp .env.example .env
# Edit .env and add FOTMOB_X_MAS_TOKEN

# 2. Start services
docker-compose up -d

# 3. Create ClickHouse tables
docker-compose exec scraper python scripts/setup_clickhouse.py

# 4. Run pipeline (data/ and logs/ folders created automatically)
docker-compose exec scraper python scripts/pipeline.py 20251208

# 5. Optimize tables (run periodically)
make optimize-tables
```

## Architecture

```
FotMob API / AIScore Web
         ↓
Bronze Layer (JSON → TAR)
         ↓
ClickHouse (Analytics)
```

**Data Sources:**
- **FotMob**: Match details, player stats, shots, timeline (REST API)
- **AIScore**: Betting odds, match info (web scraping with Selenium)

**Storage:**
- **Bronze**: Raw JSON data compressed in TAR archives (`data/fotmob/`, `data/aiscore/`)
- **ClickHouse**: Analytics tables (fotmob: 14 tables, aiscore: 5 tables)

## Installation

### Using Docker (Recommended)

```bash
# Start all services
docker-compose up -d

# Setup ClickHouse
docker-compose exec scraper python scripts/setup_clickhouse.py
```

### Local Installation

```bash
# Install package
pip install -e .

# Install with dev dependencies
pip install -e ".[dev]"

# Install with Cloudflare bypass
pip install -e ".[cloudflare]"
```

## Configuration

All configuration via `.env` file:

```bash
# Required
FOTMOB_X_MAS_TOKEN=your_token_here
FOTMOB_API_BASE_URL=https://www.fotmob.com/api/data

# ClickHouse
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=fotmob_user
CLICKHOUSE_PASSWORD=fotmob_pass

# AIScore League Filtering (95 competitions)
AISCORE_FILTER_BY_LEAGUES=true
AISCORE_ALLOWED_LEAGUES=Premier League,La Liga,Serie A,Bundesliga,Ligue 1,...

# Logging
LOG_LEVEL=INFO
```

## Usage

### Unified Pipeline

```bash
# Single date (both scrapers + ClickHouse)
docker-compose exec scraper python scripts/pipeline.py 20251208

# Date range
docker-compose exec scraper python scripts/pipeline.py --start-date 20251201 --end-date 20251207

# Monthly
docker-compose exec scraper python scripts/pipeline.py --month 202512

# Options
--force           # Force re-scrape/reload
--bronze-only     # Skip ClickHouse loading
--skip-bronze     # Skip scraping, only load to ClickHouse
--skip-fotmob     # Skip FotMob scraper
--skip-aiscore    # Skip AIScore scraper
```

### Individual Scrapers

**FotMob:**
```bash
# Scrape to bronze layer
docker-compose exec scraper python scripts/scrape_fotmob.py 20251208

# Load to ClickHouse
docker-compose exec scraper python scripts/load_clickhouse.py --scraper fotmob --date 20251208
```

**AIScore:**
```bash
# Full pipeline (links + odds)
docker-compose exec scraper python scripts/scrape_aiscore.py 20251208

# Links only (faster)
docker-compose exec scraper python scripts/scrape_aiscore.py 20251208 --links-only

# Odds only
docker-compose exec scraper python scripts/scrape_aiscore.py 20251208 --odds-only
```

## ClickHouse

### Access ClickHouse

```bash
# Command line
docker-compose exec clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass

# HTTP Interface
# URL: http://localhost:8123
# User: fotmob_user, Password: fotmob_pass
```

### Sample Queries

```sql
-- Top scorers
SELECT player_name, SUM(goals) as total_goals
FROM fotmob.player
GROUP BY player_name
ORDER BY total_goals DESC
LIMIT 10;

-- Matches by league
SELECT league_name, COUNT(*) as matches
FROM fotmob.general
GROUP BY league_name
ORDER BY matches DESC;

-- Betting odds analysis
SELECT bookmaker, AVG(home_odds), AVG(draw_odds), AVG(away_odds)
FROM aiscore.odds_1x2
GROUP BY bookmaker;
```

### Table Optimization

```bash
# Optimize all tables (run after data loading)
make optimize-tables

# Or manually
docker-compose exec -T clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass \
  < clickhouse/init/03_optimize_tables.sql
```

## Key Features

- Dual scraper support (FotMob API + AIScore web scraping)
- Bronze layer with automatic TAR compression (60-75% space savings)
- ClickHouse data warehouse with 19 tables
- Automated deduplication (ReplacingMergeTree)
- League-based filtering (95 competitions)
- Docker containerization
- Comprehensive validation system
- Unified logging pipeline
- Health checks and alerting
- Idempotent operations (safe to re-run)

## Project Structure

```
scout/
├── src/              # Source code
│   ├── scrapers/     # FotMob & AIScore scrapers
│   ├── storage/      # Bronze storage & ClickHouse client
│   ├── processors/   # Data transformation
│   ├── config/       # Configuration management
│   └── utils/        # Utilities (validation, logging, etc.)
├── scripts/          # Executable scripts
├── clickhouse/       # SQL schemas
├── data/             # Bronze layer (auto-created, TAR archives)
│   ├── fotmob/       # FotMob raw data
│   └── aiscore/      # AIScore raw data
├── logs/             # Application logs (auto-created)
├── pyproject.toml    # Modern Python project config
└── .env              # Configuration
```

**Note**: The `data/` and `logs/` directories are automatically created when you first run the pipeline.

## Troubleshooting

### Missing Data or Logs Directories

**The directories are created automatically** when you run the pipeline for the first time. No manual setup needed.

If you want to pre-create them:
```bash
# Inside Docker container
docker-compose exec scraper python scripts/ensure_directories.py

# Or manually
mkdir -p data/fotmob data/aiscore logs
```

### FotMob 404 Errors

FotMob changed API endpoint in Dec 2025:
```bash
# Update .env
FOTMOB_API_BASE_URL=https://www.fotmob.com/api/data
```

### No Matches Found (AIScore)

```bash
# Analyze league names
python scripts/analyze_leagues.py --days 30

# Test filtering
python scripts/scrape_aiscore.py 20251208 --links-only
```

### Docker Issues

```bash
# Check status
docker-compose ps

# View logs
docker-compose logs -f scraper

# Restart
docker-compose restart scraper

# Rebuild
docker-compose build --no-cache
docker-compose up -d
```

## Monitoring

```bash
# View logs
tail -f logs/pipeline_20251208.log
tail -f logs/fotmob_scraper_20251208.log
tail -f logs/aiscore_scraper_20251208.log

# Health check
docker-compose exec scraper python scripts/health_check.py

# Docker status
docker-compose ps
docker-compose logs clickhouse
```

## Performance Optimizations

**Bronze Layer:**
- Automatic TAR compression (60-75% space reduction)
- Atomic file writes
- File locking for thread safety
- Batch operations

**Scraping:**
- Browser resource blocking (images, CSS, fonts)
- Request delays to avoid rate limiting
- Connection pooling
- Parallel processing (configurable)

**ClickHouse:**
- Batch inserts via DataFrames
- Table partitioning by date
- Manual table optimization
- Deduplication via ReplacingMergeTree

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for:
- Validation system details
- Adding new scrapers
- Custom processing
- Testing and debugging

## Support

**Documentation:**
- Complete guide: [DEVELOPMENT.md](DEVELOPMENT.md)
- Project config: [pyproject.toml](pyproject.toml)

**Logs:**
- Pipeline: `logs/pipeline_*.log`
- FotMob: `logs/fotmob_scraper_*.log`
- AIScore: `logs/aiscore_scraper_*.log`

**Tools:**
- Health check: `python scripts/health_check.py`
- Validation: `python scripts/validate_fotmob_responses.py`
- League analysis: `python scripts/analyze_leagues.py`

## License

MIT

---

**Scout** - Comprehensive sports data scraping and analytics system
