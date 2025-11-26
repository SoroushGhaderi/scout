# Scout

A comprehensive data pipeline system for scraping and processing football match data from FotMob (API) and AIScore (web scraping), with ClickHouse data warehouse integration.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [ClickHouse Setup](#clickhouse-setup)
- [Docker Setup](#docker-setup)
- [Troubleshooting](#troubleshooting)
- [Scripts Reference](#scripts-reference)

## Features

- **Dual Scraper Support**: FotMob (REST API) and AIScore (web scraping)
- **Data Lakehouse Architecture**: Bronze (raw) → ClickHouse (analytics)
- **Automated Data Processing**: JSON → ClickHouse pipeline
- **Docker Support**: Complete containerized setup
- **Data Quality**: Profiling, validation, and anomaly detection
- **Parallel Processing**: Multi-threaded scraping and processing
- **Idempotent Operations**: Safe to re-run without duplicates
- **Unified Logging**: All pipeline logs consolidated in `pipeline_{date}.log`
- **ClickHouse Deduplication**: Automatic deduplication using ReplacingMergeTree engine
- **Table Optimization**: Automatic table optimization after data insertion

## Architecture

### Data Flow

```
FotMob API / AIScore Web → Bronze Layer (JSON) → ClickHouse
```

### Bronze Layer
- Raw, unprocessed data (JSON/JSON.gz)
- Compressed into TAR archives
- Manifest tracking and data profiling
- Location: `data/{scraper}/`

### ClickHouse Warehouse
- Analytics-ready data warehouse
- Separate databases: `fotmob` and `aiscore`
- Partitioned tables for optimal query performance
- **AIScore tables**: Use `ReplacingMergeTree` engine for automatic deduplication
- **FotMob tables**: Use `MergeTree` engine with post-insert optimization
- **Automatic optimization**: Tables are optimized after each data insertion

## Quick Start

### Using Docker (Recommended)

```bash
# 1. Clone repository
git clone <repository-url>
cd scout

# 2. Create .env file
cp .env.example .env
# Edit .env and add FOTMOB_X_MAS_TOKEN

# 3. Start services
docker-compose up -d

# 4. Create ClickHouse tables
docker-compose exec scraper python scripts/setup_clickhouse.py

# 5. Scrape FotMob data
docker-compose exec scraper python scripts/scrape_fotmob.py 20251113

# 6. Load to ClickHouse
docker-compose exec scraper python scripts/load_clickhouse.py --scraper fotmob --date 20251113
```

### Local Installation

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up environment
cp .env.example .env
# Edit .env with your configuration

# 3. Run scraping
python scripts/scrape_fotmob.py 20251113
```

## Installation

### Prerequisites

- Python 3.8+
- Docker and Docker Compose (for containerized setup)
- 4GB+ RAM
- 10GB+ disk space

### Docker Setup

1. **Start services:**
   ```bash
   docker-compose up -d
   ```

2. **Verify services:**
   ```bash
   docker-compose ps
   docker-compose logs -f scraper
   ```

3. **Access ClickHouse:**
   - HTTP: http://localhost:8123
   - Native: localhost:9000

### Local Setup

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

## Configuration

### Single Source of Truth: .env File

All configuration is managed through a single `.env` file in the project root. This follows industry best practices for configuration management.

**Setup:**
```bash
# 1. Copy the example file
cp .env.example .env

# 2. Edit .env with your values
# Required: FOTMOB_X_MAS_TOKEN
```

**Configuration Structure:**
- **Common**: Logging, metrics, ClickHouse connection
- **FotMob**: API settings, scraping behavior, storage paths
- **AIScore**: Browser settings, scraping behavior, storage paths

**Key Variables:**
```env
# Required
FOTMOB_X_MAS_TOKEN=your_token_here

# ClickHouse
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=fotmob_user
CLICKHOUSE_PASSWORD=fotmob_pass

# Logging
LOG_LEVEL=INFO

# Alerting (optional)
ALERT_SMTP_HOST=smtp.gmail.com
ALERT_SMTP_PORT=587
ALERT_SMTP_USER=your_email@gmail.com
ALERT_SMTP_PASSWORD=your_app_password
ALERT_FROM_EMAIL=your_email@gmail.com
ALERT_TO_EMAILS=ghaderi.soroush1995@gmail.com
```

**All available options** are documented in `.env.example`. Copy it to `.env` and customize as needed.

**Note:** YAML config files are not used. All configuration comes from the `.env` file.

### Alerting

The system includes comprehensive alerting for:
- **Failed scrapes**: Alerts when match scraping fails
- **Data quality issues**: Alerts when data validation fails
- **System failures**: Alerts from health checks (ClickHouse, storage, disk space)

**Alert Channels:**
- **Logging** (always enabled): Alerts are logged to application logs
- **Email** (optional): Configure SMTP settings in `.env`

**Configuration:**
```env
# Email alerts
ALERT_SMTP_HOST=smtp.gmail.com
ALERT_SMTP_PORT=587
ALERT_SMTP_USER=your_email@gmail.com
ALERT_SMTP_PASSWORD=your_app_password
ALERT_FROM_EMAIL=your_email@gmail.com
ALERT_TO_EMAILS=ghaderi.soroush1995@gmail.com
```

Alerts are automatically sent when issues are detected. No additional code required.

## Usage

### Unified Pipeline (Recommended)

**Run complete pipeline for both scrapers sequentially:**
```bash
# Single date (Bronze scraping + ClickHouse loading for both FotMob and AIScore)
docker-compose exec scraper python scripts/pipeline.py 20251113

# Date range
docker-compose exec scraper python scripts/pipeline.py --start-date 20251101 --end-date 20251107

# Monthly scraping
docker-compose exec scraper python scripts/pipeline.py --month 202511

# Options
docker-compose exec scraper python scripts/pipeline.py 20251113 --force          # Force re-scrape/reload
docker-compose exec scraper python scripts/pipeline.py 20251113 --bronze-only    # Bronze only (skip ClickHouse)
docker-compose exec scraper python scripts/pipeline.py 20251113 --skip-bronze     # ClickHouse only (skip scraping)
docker-compose exec scraper python scripts/pipeline.py 20251113 --skip-fotmob    # Skip FotMob entirely
docker-compose exec scraper python scripts/pipeline.py 20251113 --skip-aiscore    # Skip AIScore entirely
```

### Individual Scrapers

#### FotMob Scraper

**Bronze Layer (Raw Data):**
```bash
# Single date
docker-compose exec scraper python scripts/scrape_fotmob.py 20251113

# Date range
docker-compose exec scraper python scripts/scrape_fotmob.py 20251101 20251110

# Monthly
docker-compose exec scraper python scripts/scrape_fotmob.py --month 202511
```

**Load to ClickHouse:**
```bash
docker-compose exec scraper python scripts/load_clickhouse.py --scraper fotmob --date 20251113
docker-compose exec scraper python scripts/load_clickhouse.py --scraper fotmob --month 202511
```

#### AIScore Scraper

**Bronze Layer (Links + Odds):**
```bash
# Single date (full pipeline)
docker-compose exec scraper python scripts/scrape_aiscore.py 20251113

# Date range
docker-compose exec scraper python scripts/scrape_aiscore.py 20251101 20251107

# Monthly
docker-compose exec scraper python scripts/scrape_aiscore.py --month 202511

# Links only
docker-compose exec scraper python scripts/scrape_aiscore.py 20251113 --links-only

# Odds only
docker-compose exec scraper python scripts/aiscore_scripts/scrape_odds.py --date 20251113
```

**Load to ClickHouse:**
```bash
docker-compose exec scraper python scripts/load_clickhouse.py --scraper aiscore --date 20251113
docker-compose exec scraper python scripts/load_clickhouse.py --scraper aiscore --month 202511
```

### Data Processing

**Load to ClickHouse:**
```bash
docker-compose exec scraper python scripts/load_clickhouse.py --scraper fotmob --date 20251113
```

## ClickHouse Setup

### Initial Setup

1. **Create tables:**
   ```bash
   docker-compose exec scraper python scripts/setup_clickhouse.py
   ```

2. **Verify tables:**
   ```bash
   docker-compose exec clickhouse clickhouse-client \
     --user fotmob_user \
     --password fotmob_pass \
     --query "SHOW TABLES FROM fotmob"
   ```

### Accessing ClickHouse

**Command Line:**
```bash
docker-compose exec clickhouse clickhouse-client \
  --user fotmob_user \
  --password fotmob_pass \
  --database fotmob
```

**Python:**
```python
import clickhouse_connect

client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='fotmob_user',
    password='fotmob_pass',
    database='fotmob'
)

df = client.query_df("SELECT * FROM general LIMIT 10")
```

### Sample Queries

**Top Scorers:**
```sql
SELECT 
    player_name,
    SUM(goals) as total_goals
FROM fotmob.player
GROUP BY player_name
ORDER BY total_goals DESC
LIMIT 10;
```

**Goals Per Match:**
```sql
SELECT 
    league_name,
    COUNT(DISTINCT match_id) as matches,
    COUNT(*) as total_goals,
    ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT match_id), 2) as goals_per_match
FROM fotmob.general g
LEFT JOIN fotmob.goal gl ON g.match_id = gl.match_id
GROUP BY league_name
ORDER BY goals_per_match DESC;
```

### Tables

**FotMob Database:**
- `general` - Match overview
- `player` - Player statistics
- `shotmap` - Shot events with xG
- `goal` - Goal events
- `cards` - Card events
- `venue` - Stadium information
- `timeline` - Match timeline
- `period` - Period statistics
- `momentum` - Match momentum
- `starters` - Starting lineups
- `substitutes` - Substitute players
- `coaches` - Team coaches
- `team_form` - Team form data
- `red_card` - Red card events
- **Engine**: `MergeTree` with automatic optimization after insertion

**AIScore Database:**
- `matches` - Match information
- `odds_1x2` - 1X2 betting odds
- `odds_asian_handicap` - Asian handicap odds
- `odds_over_under` - Over/Under odds
- `daily_listings` - Daily scraping summary
- **Engine**: `ReplacingMergeTree` for automatic deduplication based on `inserted_at` timestamp

## Docker Setup

### Services

- **clickhouse**: ClickHouse database server
- **scraper**: Main application container
- **scheduler**: Optional scheduled scraping (disabled by default)

### Common Commands

**Start services:**
```bash
docker-compose up -d
```

**View logs:**
```bash
docker-compose logs -f scraper
docker-compose logs -f clickhouse
```

**Enter container:**
```bash
docker-compose exec scraper bash
```

**Stop services:**
```bash
docker-compose down
```

**Rebuild:**
```bash
docker-compose build --no-cache
docker-compose up -d
```

### Volume Mounts

- `./data` → `/app/data` - Persistent data storage
- `./logs` → `/app/logs` - Application logs
- `./config` → `/app/config` - Configuration files
- `./scripts` → `/app/scripts` - Scripts directory
- `./src` → `/app/src` - Source code

## Troubleshooting

### Docker Issues

**Docker Desktop not running:**
- Start Docker Desktop application
- Wait for it to fully initialize
- Verify with: `docker ps`

**Container won't start:**
```bash
docker-compose logs scraper
docker-compose restart scraper
```

**ClickHouse connection errors:**
```bash
# Check if ClickHouse is healthy
docker-compose exec clickhouse clickhouse-client --query "SELECT 1"

# Check logs
docker-compose logs clickhouse
```

### Data Loading Issues

**Tables not created:**
```bash
docker-compose exec scraper python scripts/setup_clickhouse.py
```

**Missing data:**
- Verify bronze files exist: `ls data/fotmob/matches/`
- Check logs: `docker-compose logs scraper`
- Verify date format: YYYYMMDD (e.g., 20251113)

**Type errors:**
- Ensure tables are created with correct schema
- Check data types match table definitions
- Review error logs for specific issues

### Scraping Issues

**FotMob API errors:**
- Verify `FOTMOB_X_MAS_TOKEN` in `.env`
- Check rate limiting settings
- Review API response in logs

**AIScore browser errors:**
- Check Chrome/Chromium installation
- Verify headless mode settings
- Review browser logs

## Scripts Reference

### Core Scripts

**Data Scraping:**
- `scripts/scrape_fotmob.py` - Scrape FotMob data (bronze layer)
- `scripts/scrape_aiscore.py` - Scrape AIScore data (bronze layer)

**Data Loading:**
- `scripts/load_clickhouse.py` - Load data to ClickHouse
- `scripts/setup_clickhouse.py` - Initialize ClickHouse tables

**Pipeline Orchestration:**
- `scripts/pipeline.py` - Run complete pipeline (orchestrator)
- `scripts/pipeline.py` - Run complete pipeline (orchestrator)

**AIScore Scraping:**
- `scripts/aiscore_scripts/scrape_links.py` - Scrape match links
- `scripts/aiscore_scripts/scrape_odds.py` - Scrape odds data

### Script Usage

**Load data to ClickHouse:**
```bash
# Single date
python scripts/load_clickhouse.py --scraper fotmob --date 20251113

# Date range
python scripts/load_clickhouse.py --scraper fotmob --start-date 20251101 --end-date 20251110

# Show statistics
python scripts/load_clickhouse.py --scraper fotmob --stats
```

**Create tables:**
```bash
python scripts/setup_clickhouse.py
```

## Data Structure

### Bronze Layer

```
data/
├── fotmob/
│   ├── matches/
│   │   └── YYYYMMDD/
│   │       ├── match_*.json (individual files)
│   │       └── YYYYMMDD_matches.tar (compressed archive)
│   ├── lineage/
│   │   └── YYYYMMDD/
│   │       └── lineage.json
│   └── daily_listings/
│       └── YYYYMMDD/
│           └── matches.json (with storage statistics)
└── aiscore/
    ├── matches/
    │   └── YYYYMMDD/
    │       ├── match_*.json.gz (compressed files)
    │       └── YYYYMMDD_matches.tar (compressed archive)
    ├── lineage/
    │   └── YYYYMMDD/
    │       └── lineage.json
    └── daily_listings/
        └── YYYYMMDD/
            └── matches.json (with scrape status)
```

### Logs

```
logs/
├── pipeline_YYYYMMDD.log (unified pipeline logs)
├── pipeline_YYYYMMDD_to_YYYYMMDD.log (date range)
├── pipeline_YYYYMM.log (monthly)
├── fotmob_scraper_YYYYMMDD.log (individual scraper logs)
└── aiscore_scraper_YYYYMMDD.log (individual scraper logs)
```

## Development

### Project Structure

```
scout/
├── src/
│   ├── scrapers/          # Scraper implementations
│   ├── storage/            # Storage layer
│   ├── processors/         # Data processors
│   ├── config/             # Configuration
│   └── utils/              # Utilities
├── scripts/                # Executable scripts
├── config/                 # Configuration files
├── clickhouse/             # ClickHouse SQL scripts
├── data/                   # Data storage
└── logs/                   # Application logs
```

### Running Tests

```bash
pytest tests/
```

## License

[Add your license information here]

## Logging

### Unified Pipeline Logs

All pipeline execution logs are consolidated into a single file:
- **Format**: `pipeline_{date}.log`
- **Single date**: `pipeline_20251115.log`
- **Date range**: `pipeline_20251101_to_20251107.log`
- **Monthly**: `pipeline_202511.log`
- **Location**: `logs/pipeline_*.log`

The unified log includes:
- All subprocess output (FotMob scraping, AIScore scraping, ClickHouse loading)
- Pipeline orchestration messages
- Error handling and alerts
- Summary statistics

### Individual Scraper Logs

Individual scrapers also create their own log files:
- FotMob: `logs/fotmob_scraper_YYYYMMDD.log`
- AIScore: `logs/aiscore_scraper_YYYYMMDD.log`

## Data Deduplication

### AIScore Tables
- **Engine**: `ReplacingMergeTree(inserted_at)`
- **Deduplication**: Automatic via ClickHouse engine
- **Method**: Keeps row with highest `inserted_at` timestamp
- **Optimization**: `OPTIMIZE TABLE FINAL` runs after each insertion

### FotMob Tables
- **Engine**: `MergeTree`
- **Deduplication**: Python-side deduplication removed (inserts all data)
- **Optimization**: `OPTIMIZE TABLE FINAL` runs after each insertion to merge parts
- **Note**: For deduplication, consider migrating to `ReplacingMergeTree` if needed

## Support

For issues and questions:
- Check logs: `logs/` directory (especially `pipeline_*.log` for unified view)
- Review troubleshooting section
- Check Docker logs: `docker-compose logs`

