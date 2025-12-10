# Scout

> **Football Data Pipeline** - Scraping and processing match data from FotMob (API) and AIScore (web scraping) with ClickHouse data warehouse integration.

## 🚀 Quick Start

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

# 4. Run pipeline
docker-compose exec scraper python scripts/pipeline.py 20251208

# 5. Optimize tables (optional, run periodically)
make optimize-tables
```

## 📖 Documentation

**Complete documentation:** [DOCUMENTATION.md](DOCUMENTATION.md)

## ⚡ Common Commands

```bash
# Run full pipeline (both scrapers + ClickHouse)
docker-compose exec scraper python scripts/pipeline.py 20251208

# Date range
docker-compose exec scraper python scripts/pipeline.py --start-date 20251201 --end-date 20251207

# Monthly scraping
docker-compose exec scraper python scripts/pipeline.py --month 202512

# FotMob only
docker-compose exec scraper python scripts/scrape_fotmob.py 20251208

# AIScore only
docker-compose exec scraper python scripts/scrape_aiscore.py 20251208

# View logs
docker-compose logs -f scraper
tail -f logs/pipeline_20251208.log

# Access ClickHouse
docker-compose exec clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass

# Optimize and deduplicate tables (run after data loading)
make optimize-tables
```

## 🏗️ Architecture

```
FotMob API / AIScore Web
         ↓
Bronze Layer (JSON)
         ↓
ClickHouse (Analytics)
```

### Data Sources

- **FotMob**: Match details, player stats, shots, timeline (REST API)
- **AIScore**: Betting odds, match info (web scraping)

### Storage Layers

- **Bronze**: Raw JSON data in `data/{fotmob|aiscore}/`
- **ClickHouse**: Analytics-ready tables
  - **fotmob** database: 14 tables (general, player, shotmap, goal, etc.)
  - **aiscore** database: 5 tables (matches, odds_1x2, odds_asian_handicap, odds_over_under)

## ⚙️ Configuration

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

# League Filtering (AIScore)
AISCORE_FILTER_BY_LEAGUES=true
AISCORE_ALLOWED_LEAGUES=Premier League,La Liga,Serie A,Bundesliga,Ligue 1,...

# Logging
LOG_LEVEL=INFO
```

See [DOCUMENTATION.md](DOCUMENTATION.md#configuration) for all options.

## 🔧 Troubleshooting

### FotMob 404 Errors

**Updated Dec 2025:** FotMob changed API endpoint
```bash
# Update .env
FOTMOB_API_BASE_URL=https://www.fotmob.com/api/data
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

### No Matches Found (AIScore)

```bash
# Analyze league names
python scripts/analyze_leagues.py --days 30

# Test filtering
python scripts/scrape_aiscore.py 20251208 --links-only
```

## 📊 ClickHouse Queries

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

-- Betting odds
SELECT bookmaker, AVG(home_odds), AVG(draw_odds), AVG(away_odds)
FROM aiscore.odds_1x2
GROUP BY bookmaker;
```

## 📁 Project Structure

```
scout/
├── src/              # Source code
│   ├── scrapers/     # FotMob & AIScore scrapers
│   ├── storage/      # Bronze storage & ClickHouse
│   ├── processors/   # Data transformation
│   ├── config/       # Configuration management
│   └── utils/        # Utilities
├── scripts/          # Executable scripts
├── clickhouse/       # SQL schemas
├── data/             # Bronze layer data
├── logs/             # Application logs
├── .env              # Configuration
└── DOCUMENTATION.md  # Complete documentation
```

## 🔍 Key Features

- ✅ Dual scraper support (FotMob API + AIScore web scraping)
- ✅ Bronze layer with TAR compression
- ✅ ClickHouse data warehouse
- ✅ Automated deduplication (ReplacingMergeTree)
- ✅ League-based filtering (95 competitions)
- ✅ Docker containerization
- ✅ Unified logging pipeline
- ✅ Health checks and alerting
- ✅ Idempotent operations (safe to re-run)

## 📚 Documentation

- **[DOCUMENTATION.md](DOCUMENTATION.md)** - Complete guide (architecture, usage, troubleshooting)

## 🆘 Support

**Logs:**
```bash
# Unified pipeline logs
tail -f logs/pipeline_20251208.log

# Individual scrapers
tail -f logs/fotmob_scraper_20251208.log
tail -f logs/aiscore_scraper_20251208.log
```

**Health Check:**
```bash
docker-compose exec scraper python scripts/health_check.py
```

**Docker Status:**
```bash
docker-compose ps
docker-compose logs clickhouse
```

---

**For detailed information, see [DOCUMENTATION.md](DOCUMENTATION.md)**
