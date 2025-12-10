# Scout - Complete Documentation

> **Football Data Pipeline System** - Scraping and processing match data from FotMob (API) and AIScore (web scraping) with ClickHouse data warehouse integration.

**Last Updated:** December 8, 2025

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Architecture](#architecture)
3. [Configuration](#configuration)
4. [Usage Guide](#usage-guide)
5. [ClickHouse Setup](#clickhouse-setup)
6. [Troubleshooting](#troubleshooting)
7. [Advanced Topics](#advanced-topics)

---

## Quick Start

### Using Docker (Recommended)

```bash
# 1. Clone and setup
git clone <repository-url>
cd scout
cp .env.example .env
# Edit .env and add FOTMOB_X_MAS_TOKEN

# 2. Start services
docker-compose up -d

# 3. Create ClickHouse tables
docker-compose exec scraper python scripts/setup_clickhouse.py

# 4. Run pipeline
docker-compose exec scraper python scripts/pipeline.py 20251208
```

### Local Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your settings

# Run scraping
python scripts/scrape_fotmob.py 20251208
```

---

## Architecture

### High-Level Overview

```
┌─────────────────┐         ┌─────────────────┐
│   FotMob API    │         │   AIScore Web   │
└────────┬────────┘         └────────┬────────┘
         │                            │
         ▼                            ▼
┌─────────────────────────────────────────────┐
│         Bronze Layer (Raw JSON)             │
│  ┌──────────────┐      ┌──────────────┐    │
│  │ data/fotmob/ │      │ data/aiscore/│    │
│  │  - matches/  │      │  - matches/  │    │
│  │  - lineage/  │      │  - lineage/  │    │
│  │  - daily_    │      │  - daily_    │    │
│  │    listings/ │      │    listings/ │    │
│  └──────────────┘      └──────────────┘    │
└─────────────────────────────────────────────┘
         │                            │
         ▼                            ▼
┌─────────────────────────────────────────────┐
│         ClickHouse Data Warehouse           │
│  ┌──────────────┐      ┌──────────────┐    │
│  │ fotmob DB    │      │ aiscore DB   │    │
│  │  14 tables   │      │  5 tables    │    │
│  └──────────────┘      └──────────────┘    │
└─────────────────────────────────────────────┘
```

### Data Flow

**Bronze Layer Scraping:**
```
1. Fetch Daily Listing → API/Web
2. Get Match IDs → Filter already scraped
3. Scrape Matches → Parallel/Sequential
4. Save to Bronze → Atomic writes + metadata
5. Post-Processing → Compress + Update listing
```

**ClickHouse Loading:**
```
1. Load Bronze Data → From TAR or files
2. Process Data → Parse JSON to DataFrames
3. Validate & Transform → Types + timestamps
4. Insert to ClickHouse → Batch insert
5. Optimize Tables → OPTIMIZE TABLE FINAL
```

### Project Structure

```
scout/
├── src/
│   ├── scrapers/          # FotMob & AIScore scrapers
│   ├── storage/           # Bronze storage & ClickHouse client
│   ├── processors/        # Data transformation
│   ├── config/            # Configuration management
│   └── utils/             # Utilities (logging, health checks, etc.)
├── scripts/               # Executable scripts
├── clickhouse/            # ClickHouse SQL schemas
├── data/                  # Bronze layer storage
├── logs/                  # Application logs
└── .env                   # Configuration (single source of truth)
```

### Key Components

**Scrapers:**
- **FotMob**: REST API scraper with retry logic and rate limiting
- **AIScore**: Selenium-based web scraper with browser pooling

**Storage:**
- **Bronze Layer**: Raw JSON storage with compression (TAR archives)
- **ClickHouse**: Analytics-ready data warehouse

**Processors:**
- **Match Processor**: Transforms FotMob API data to DataFrames
- **Odds Parsers**: Parse AIScore HTML to structured odds data

---

## Configuration

### Environment Variables (.env)

**All configuration is managed through `.env` file** (single source of truth)

```bash
# Required - FotMob API Token
FOTMOB_X_MAS_TOKEN=your_token_here

# ClickHouse Connection
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=fotmob_user
CLICKHOUSE_PASSWORD=fotmob_pass

# Logging
LOG_LEVEL=INFO

# FotMob API Base URL (IMPORTANT - Updated Dec 2025)
FOTMOB_API_BASE_URL=https://www.fotmob.com/api/data

# Email Alerts (Optional)
ALERT_SMTP_HOST=smtp.gmail.com
ALERT_SMTP_PORT=587
ALERT_SMTP_USER=your_email@gmail.com
ALERT_SMTP_PASSWORD=your_app_password
ALERT_FROM_EMAIL=your_email@gmail.com
ALERT_TO_EMAILS=recipient@gmail.com
```

### League Filtering (AIScore)

**Current Strategy: League-Based Filtering** (95 competitions)

```bash
# Enable league filtering
AISCORE_FILTER_BY_LEAGUES=true

# Disable country filtering
AISCORE_FILTER_BY_COUNTRIES=false

# Specify leagues (comma-separated)
AISCORE_ALLOWED_LEAGUES=Premier League,La Liga,Serie A,Bundesliga,Ligue 1,Champions League,UEFA Europa League,UEFA Europa Conference League,...
```

**Coverage Breakdown:**
- 35 Men's top leagues (top 2 from 12 countries)
- 15 Major domestic cups
- 8 International club competitions (UEFA + CAF)
- 11 International national team tournaments
- 21 Women's competitions
- 6 Youth leagues

**Performance Impact:**
- ~5,500 matches/month (vs ~15,000 unfiltered)
- 63% storage reduction
- Professional + International + Youth + Women's coverage

**Alternative: Country-Based Filtering**

```bash
AISCORE_FILTER_BY_LEAGUES=false
AISCORE_FILTER_BY_COUNTRIES=true
AISCORE_ALLOWED_COUNTRIES=England,Spain,Germany,Italy,France,Portugal,...
```

### Browser Configuration (AIScore)

```bash
# Headless mode
AISCORE_HEADLESS=true

# Performance optimizations
AISCORE_BROWSER_BLOCK_IMAGES=true
AISCORE_BROWSER_BLOCK_CSS=true
AISCORE_BROWSER_BLOCK_FONTS=true
AISCORE_BROWSER_BLOCK_MEDIA=true

# Timeouts (seconds)
AISCORE_TIMEOUT_PAGE_LOAD=30
AISCORE_TIMEOUT_ELEMENT_WAIT=10
AISCORE_TIMEOUT_CLOUDFLARE_MAX=15

# Delays (seconds)
AISCORE_DELAY_BETWEEN_DATES=1.0
AISCORE_DELAY_BETWEEN_MATCHES=0.5
```

---

## Usage Guide

### Unified Pipeline (Recommended)

```bash
# Single date (Bronze + ClickHouse for both FotMob and AIScore)
docker-compose exec scraper python scripts/pipeline.py 20251208

# Date range
docker-compose exec scraper python scripts/pipeline.py --start-date 20251201 --end-date 20251207

# Monthly
docker-compose exec scraper python scripts/pipeline.py --month 202512

# Options
--force           # Force re-scrape/reload
--bronze-only     # Skip ClickHouse loading
--skip-bronze     # Skip scraping, only load to ClickHouse
--skip-fotmob     # Skip FotMob entirely
--skip-aiscore    # Skip AIScore entirely
```

### Individual Scrapers

**FotMob:**
```bash
# Scrape to Bronze
docker-compose exec scraper python scripts/scrape_fotmob.py 20251208

# Date range
docker-compose exec scraper python scripts/scrape_fotmob.py 20251201 20251207

# Monthly
docker-compose exec scraper python scripts/scrape_fotmob.py --month 202512

# Load to ClickHouse
docker-compose exec scraper python scripts/load_clickhouse.py --scraper fotmob --date 20251208
```

**AIScore:**
```bash
# Full pipeline (links + odds)
docker-compose exec scraper python scripts/scrape_aiscore.py 20251208

# Links only (faster)
docker-compose exec scraper python scripts/scrape_aiscore.py 20251208 --links-only

# Odds only (requires links first)
docker-compose exec scraper python scripts/scrape_aiscore.py 20251208 --odds-only

# Visible browser (debugging)
docker-compose exec scraper python scripts/scrape_aiscore.py 20251208 --visible

# Load to ClickHouse
docker-compose exec scraper python scripts/load_clickhouse.py --scraper aiscore --date 20251208
```

### Common Commands

```bash
# View logs
docker-compose logs -f scraper
docker-compose logs -f clickhouse

# Enter container
docker-compose exec scraper bash

# Check service status
docker-compose ps

# Restart services
docker-compose restart scraper

# Stop all services
docker-compose down

# Rebuild (after code changes)
docker-compose build --no-cache
docker-compose up -d
```

---

## ClickHouse Setup

### Initial Setup

```bash
# Create databases and tables
docker-compose exec scraper python scripts/setup_clickhouse.py

# Verify tables
docker-compose exec clickhouse clickhouse-client \
  --user fotmob_user \
  --password fotmob_pass \
  --query "SHOW TABLES FROM fotmob"
```

### Database Schema

**FotMob Database (14 tables):**
- `general` - Match overview
- `player` - Player statistics
- `shotmap` - Shot events with xG
- `goal` - Goal events
- `cards` - Card events
- `red_card` - Red card events
- `venue` - Stadium information
- `timeline` - Match timeline
- `period` - Period statistics
- `momentum` - Match momentum
- `starters` - Starting lineups
- `substitutes` - Substitute players
- `coaches` - Team coaches
- `team_form` - Team form data

**Engine:** `MergeTree` with automatic optimization

**AIScore Database (5 tables):**
- `matches` - Match information
- `odds_1x2` - 1X2 betting odds
- `odds_asian_handicap` - Asian handicap odds
- `odds_over_under` - Over/Under odds
- `daily_listings` - Daily scraping summary

**Engine:** `ReplacingMergeTree(inserted_at)` for automatic deduplication

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

**HTTP Interface:**
- URL: http://localhost:8123
- User: fotmob_user
- Password: fotmob_pass

### Table Optimization

After loading data, you should periodically optimize tables to deduplicate rows and merge data parts. This improves query performance and reduces storage usage.

**Using Makefile (Recommended):**
```bash
# Optimize all tables
make optimize-tables
```

**Using ClickHouse Client:**
```bash
# Run optimization script
docker-compose exec -T clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass \
  < clickhouse/init/03_optimize_tables.sql
```

**Manual Optimization:**
```bash
# Optimize specific table
docker-compose exec clickhouse clickhouse-client \
  --user fotmob_user \
  --password fotmob_pass \
  --query "OPTIMIZE TABLE fotmob.general FINAL DEDUPLICATE"
```

**When to Optimize:**
- After bulk data loading
- Periodically (e.g., daily or weekly)
- When you notice slow query performance
- After deleting/updating large amounts of data

**Note:** The data loading script no longer performs automatic optimization. This separation allows you to control when optimization occurs and reduces load time.

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

**Goals Per Match by League:**
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

**Betting Odds Analysis:**
```sql
SELECT 
    bookmaker,
    AVG(home_odds) as avg_home_odds,
    AVG(draw_odds) as avg_draw_odds,
    AVG(away_odds) as avg_away_odds
FROM aiscore.odds_1x2
GROUP BY bookmaker
ORDER BY bookmaker;
```

---

## Troubleshooting

### FotMob API Issues

**Problem:** 404 errors when fetching matches

**Solution:** FotMob changed their API endpoint in Dec 2025
```bash
# Update .env with new endpoint
FOTMOB_API_BASE_URL=https://www.fotmob.com/api/data
```

**Problem:** Invalid or expired token

**Solution:** Get new `x-mas` token from FotMob website
1. Open https://www.fotmob.com in browser
2. Open DevTools → Network tab
3. Refresh page and look for API requests
4. Find `x-mas` header value
5. Update `FOTMOB_X_MAS_TOKEN` in `.env`

### AIScore Scraping Issues

**Problem:** No matches found

**Solution:** Check league filtering configuration
```bash
# Verify league names match exactly
python scripts/analyze_leagues.py --days 30

# Test with single league
AISCORE_ALLOWED_LEAGUES=Premier League
python scripts/scrape_aiscore.py 20251208 --links-only
```

**Problem:** Browser timeout errors

**Solution:** Increase timeouts in `.env`
```bash
AISCORE_TIMEOUT_PAGE_LOAD=60
AISCORE_TIMEOUT_ELEMENT_WAIT=20
```

**Problem:** Cloudflare blocking

**Solution:**
1. Disable headless mode temporarily: `AISCORE_HEADLESS=false`
2. Increase Cloudflare timeout: `AISCORE_TIMEOUT_CLOUDFLARE_MAX=30`
3. Add longer delays: `AISCORE_DELAY_BETWEEN_DATES=2.0`

### Docker Issues

**Problem:** Docker Desktop not running

**Solution:**
```bash
# Start Docker Desktop
# Verify with:
docker ps
```

**Problem:** Container won't start

**Solution:**
```bash
# Check logs
docker-compose logs scraper

# Restart container
docker-compose restart scraper

# Rebuild if needed
docker-compose build --no-cache scraper
docker-compose up -d
```

**Problem:** ClickHouse connection errors

**Solution:**
```bash
# Check ClickHouse health
docker-compose exec clickhouse clickhouse-client --query "SELECT 1"

# Check logs
docker-compose logs clickhouse

# Verify connection settings in .env
```

### Data Loading Issues

**Problem:** Tables not created

**Solution:**
```bash
# Re-run setup
docker-compose exec scraper python scripts/setup_clickhouse.py

# Verify
docker-compose exec clickhouse clickhouse-client \
  --user fotmob_user \
  --password fotmob_pass \
  --query "SHOW DATABASES"
```

**Problem:** Missing data

**Solution:**
```bash
# Verify bronze files exist
ls data/fotmob/matches/20251208/
ls data/aiscore/matches/20251208/

# Check logs
tail -f logs/pipeline_20251208.log

# Force reload
docker-compose exec scraper python scripts/load_clickhouse.py \
  --scraper fotmob --date 20251208 --force
```

---

## Advanced Topics

### Data Deduplication

**AIScore Tables:**
- Engine: `ReplacingMergeTree(inserted_at)`
- Automatic deduplication by ClickHouse
- Keeps row with highest `inserted_at` timestamp
- Optimization runs after each insertion

**FotMob Tables:**
- Engine: `MergeTree`
- Optimization runs after insertion to merge parts
- No expected duplicates (each match scraped once)

### Performance Optimization

**Bronze Layer:**
- Atomic file writes (temp → rename pattern)
- TAR compression for storage efficiency
- File locking for thread safety
- Batch operations for better I/O

**Scraping:**
- Browser resource blocking (images, CSS, fonts)
- Request delays to avoid rate limiting
- Connection pooling and reuse
- Parallel processing (configurable)

**ClickHouse:**
- Batch inserts via DataFrames
- Table partitioning by date
- Automatic table optimization
- Deduplication via ReplacingMergeTree

### Health Checks

**Pre-flight Checks:**
- Disk space availability
- Write permissions
- Network connectivity
- ClickHouse connection

**Runtime Monitoring:**
- Scraper metrics (success/failure counts)
- Data quality validation
- Alert system (email + logs)

**Manual Health Check:**
```bash
docker-compose exec scraper python scripts/health_check.py
```

### Alerting System

**Alert Types:**
- Failed scrapes
- Data quality issues
- System failures (disk space, ClickHouse down)

**Alert Channels:**
- **Logging** (always enabled)
- **Email** (optional, configure SMTP in .env)

**Configuration:**
```bash
# Email alerts
ALERT_SMTP_HOST=smtp.gmail.com
ALERT_SMTP_PORT=587
ALERT_SMTP_USER=your_email@gmail.com
ALERT_SMTP_PASSWORD=your_app_password
ALERT_FROM_EMAIL=your_email@gmail.com
ALERT_TO_EMAILS=recipient@gmail.com
```

### League Analysis

**Analyze scraped data to find league names:**
```bash
# Analyze last 30 days
python scripts/analyze_leagues.py --days 30

# Custom date range
python scripts/analyze_leagues.py --start-date 20251101 --days 30

# Show top 100 leagues
python scripts/analyze_leagues.py --top-n 100

# Set minimum matches threshold
python scripts/analyze_leagues.py --min-matches 50
```

### Data Structure

**Bronze Layer:**
```
data/
├── fotmob/
│   ├── matches/
│   │   └── YYYYMMDD/
│   │       ├── match_*.json
│   │       └── YYYYMMDD_matches.tar
│   ├── lineage/
│   │   └── YYYYMMDD/
│   │       └── lineage.json
│   └── daily_listings/
│       └── YYYYMMDD/
│           └── matches.json
└── aiscore/
    ├── matches/
    │   └── YYYYMMDD/
    │       ├── match_*.json.gz
    │       └── YYYYMMDD_matches.tar
    ├── lineage/
    │   └── YYYYMMDD/
    │       └── lineage.json
    └── daily_listings/
        └── YYYYMMDD/
            └── matches.json
```

**Logs:**
```
logs/
├── pipeline_YYYYMMDD.log              # Unified pipeline logs
├── pipeline_YYYYMMDD_to_YYYYMMDD.log  # Date range
├── pipeline_YYYYMM.log                # Monthly
├── fotmob_scraper_YYYYMMDD.log        # Individual scraper
└── aiscore_scraper_YYYYMMDD.log       # Individual scraper
```

### Security Considerations

**SQL Injection Protection:**
- Table name whitelist in ClickHouseClient
- Parameterized queries
- Input validation

**File System Security:**
- Atomic writes (temp → rename)
- File locking for concurrent access
- Path validation

**Credentials Management:**
- Environment variables (.env file)
- No hardcoded secrets
- Docker secrets support

### Extension Points

**Adding New Scrapers:**
1. Create scraper module in `src/scrapers/{scraper_name}/`
2. Implement base scraper interface
3. Add configuration in `src/config/{scraper_name}_config.py`
4. Update pipeline script
5. Create ClickHouse schema

**Adding New Data Sources:**
1. Extend BronzeStorage or create scraper-specific storage
2. Add data models in `src/models/`
3. Create processor if transformation needed
4. Update ClickHouse schema

**Custom Processing:**
1. Extend MatchProcessor or create new processor
2. Add validation rules in `src/utils/validation.py`
3. Integrate into orchestrator workflow

---

## Quick Reference

### Most Common Commands

```bash
# Run full pipeline (today's date)
docker-compose exec scraper python scripts/pipeline.py $(date +%Y%m%d)

# Run full pipeline (specific date)
docker-compose exec scraper python scripts/pipeline.py 20251208

# Scrape only (no ClickHouse)
docker-compose exec scraper python scripts/pipeline.py 20251208 --bronze-only

# Load only (no scraping)
docker-compose exec scraper python scripts/pipeline.py 20251208 --skip-bronze

# Check logs
tail -f logs/pipeline_$(date +%Y%m%d).log

# Access ClickHouse
docker-compose exec clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass
```

### Environment Variable Quick Reference

```bash
# Essential
FOTMOB_X_MAS_TOKEN=<your_token>
FOTMOB_API_BASE_URL=https://www.fotmob.com/api/data
CLICKHOUSE_HOST=clickhouse
LOG_LEVEL=INFO

# AIScore Filtering
AISCORE_FILTER_BY_LEAGUES=true
AISCORE_ALLOWED_LEAGUES=Premier League,La Liga,...

# Performance
AISCORE_HEADLESS=true
AISCORE_BROWSER_BLOCK_IMAGES=true
```

### Key File Locations

- **Configuration:** `.env`
- **Logs:** `logs/pipeline_*.log`
- **Bronze Data:** `data/{fotmob|aiscore}/matches/YYYYMMDD/`
- **Scripts:** `scripts/*.py`
- **Source Code:** `src/`

---

## Dependencies

### Core Libraries

- **selenium** - Web scraping (AIScore)
- **requests** - HTTP client (FotMob)
- **pandas** - Data processing
- **clickhouse-connect** - ClickHouse client
- **pydantic** - Data validation
- **beautifulsoup4** - HTML parsing

### Infrastructure

- **Docker** - Containerization
- **ClickHouse** - Data warehouse
- **Chrome/Chromium** - Browser automation

---

## Support & Resources

**Log Files:**
- Pipeline: `logs/pipeline_*.log`
- FotMob: `logs/fotmob_scraper_*.log`
- AIScore: `logs/aiscore_scraper_*.log`

**Health Check:**
```bash
docker-compose exec scraper python scripts/health_check.py
```

**Docker Status:**
```bash
docker-compose ps
docker-compose logs -f scraper
```

---

**Scout Data Pipeline** - Comprehensive football data scraping and analytics system

