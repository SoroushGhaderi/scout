# Scout

Scout is a FotMob-only football data pipeline with an explicit medallion architecture.

- Bronze: raw FotMob API responses stored on disk and loaded into ClickHouse `bronze_*` tables
- Silver: cleaned ClickHouse `silver_*` views built from Bronze
- Gold: analytics-ready ClickHouse `gold_*` tables built from Silver

## Architecture

```text
FotMob API
  -> data/fotmob/                   raw Bronze files
  -> fotmob.bronze_*               ClickHouse Bronze tables
  -> fotmob.silver_*               ClickHouse Silver views
  -> fotmob.gold_*                 ClickHouse Gold tables
```

## Layer Rules

- Bronze is the only filesystem-backed layer
- Silver and Gold exist only in ClickHouse
- Bronze warehouse tables are always prefixed with `bronze_`
- Silver warehouse views are always prefixed with `silver_`
- Gold warehouse tables are always prefixed with `gold_`
- Scout currently supports FotMob only

## Prerequisites

- Docker and Docker Compose
- Python 3.11 if running locally outside Docker
- A valid `FOTMOB_X_MAS_TOKEN`
- ClickHouse credentials in `.env`

## Quick Start

```bash
git clone <repository-url>
cd scout
cp .env.example .env
# edit .env and set FOTMOB_X_MAS_TOKEN plus ClickHouse credentials

docker-compose -f docker/docker-compose.yml up -d

# create Bronze + Silver + Gold schema
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/setup_clickhouse.py

# run a complete single-day pipeline
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208
```

## Required Configuration

### `.env`

Minimum useful values:

```bash
FOTMOB_X_MAS_TOKEN=your_token_here
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=fotmob_user
CLICKHOUSE_PASSWORD=fotmob_pass
CLICKHOUSE_DB_FOTMOB=fotmob
```

### `config.yaml`

Bronze is the only local layer path:

```yaml
fotmob:
  storage:
    bronze_path: data/fotmob
    enabled: true
```

See `DEVELOPMENT_ARCHITECTURE.md` for the full development, architecture, and configuration reference.

## Running The Code

### 1. Start infrastructure

```bash
docker-compose -f docker/docker-compose.yml up -d
```

### 2. Create ClickHouse schema

Create all layers:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/setup_clickhouse.py
```

Or create one layer at a time:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/bronze/setup_clickhouse.py
docker-compose -f docker/docker-compose.yml exec scraper python scripts/silver/setup_clickhouse.py
docker-compose -f docker/docker-compose.yml exec scraper python scripts/gold/setup_clickhouse.py
```

### 3. Scrape Bronze files

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/bronze/scrape_fotmob.py 20251208
```

This writes raw FotMob match responses into `data/fotmob/`.

### 4. Load Bronze files into ClickHouse Bronze tables

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/bronze/load_clickhouse.py --scraper fotmob --date 20251208
```

This creates or appends records in tables such as:

- `fotmob.bronze_general`
- `fotmob.bronze_player`
- `fotmob.bronze_shotmap`
- `fotmob.bronze_goal`
- `fotmob.bronze_period`

### 5. Build Silver views

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/silver/process.py --date 20251208
```

This refreshes views such as:

- `fotmob.silver_general`
- `fotmob.silver_player`
- `fotmob.silver_shotmap`
- `fotmob.silver_period`
- `fotmob.silver_venue`

It also refreshes silver scenario tables (via `scripts/silver/scenario_*.py`), including:

- `fotmob.silver_scenario_the_black_hole`
- `fotmob.silver_scenario_high_line_trap`
- `fotmob.silver_scenario_the_ghost_poacher`

### 6. Build Gold tables

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/gold/process.py --date 20251208
```

This refreshes tables such as:

- `fotmob.gold_player_match_stats`
- `fotmob.gold_match_summary`
- `fotmob.gold_team_season_stats`

## Full Pipeline Modes

### One date

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208
```

### Date range

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py --start-date 20251201 --end-date 20251207
```

### Month

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py --month 202512
```

### Bronze only

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208 --bronze-only
```

### Silver only

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208 --silver-only
```

### Gold only

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208 --gold-only
```

### Skip scraping and reuse existing Bronze files

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208 --skip-bronze
```

## Bronze Table Engine

All ClickHouse Bronze tables use:

```sql
ENGINE = ReplacingMergeTree(inserted_at)
```

That allows re-runs and deduplication by keeping the newest inserted version before compaction.

Run optimization periodically:

```bash
docker-compose -f docker/docker-compose.yml exec -T clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass \
  < clickhouse/bronze/02_optimize.sql
```

## Project Structure

```text
scout/
├── clickhouse/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── config/
├── scripts/
│   ├── bronze/
│   │   ├── scrape_fotmob.py
│   │   ├── load_clickhouse.py
│   │   └── setup_clickhouse.py
│   ├── silver/
│   │   ├── process.py
│   │   └── setup_clickhouse.py
│   ├── gold/
│   │   ├── process.py
│   │   └── setup_clickhouse.py
│   ├── orchestration/
│   │   ├── pipeline.py
│   │   └── setup_clickhouse.py
│   ├── ensure_directories.py
│   ├── health_check.py
│   ├── refresh_turnstile.py
│   └── check_logging_style.py
├── src/
│   ├── processors/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── storage/
│       ├── bronze/
│       ├── silver/
│       └── gold/
└── data/
    └── fotmob/
```

## Troubleshooting

### Create local directories early

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/ensure_directories.py
```

### Check system health

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/health_check.py
docker-compose -f docker/docker-compose.yml exec scraper python scripts/health_check.py --json
```

### Recreate schema

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/setup_clickhouse.py
```

## Notes

- Scout is currently FotMob-only
- Silver and Gold are warehouse layers, not local directories
- Bare ClickHouse table names like `general` or `player` should not be introduced for warehouse objects

## Repo Hygiene

- Script inventory: `SCRIPTS_AUDIT.md`
- Script command contract: `SCRIPTS_CONTRACT.md`
