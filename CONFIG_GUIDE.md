# Configuration Guide

This guide explains how Scout is configured and how to run the FotMob-only Bronze, Silver, and Gold pipeline in practice.

## 1. Configuration Model

Scout uses two configuration sources:

- `config.yaml` for application behavior
- `.env` for secrets and environment-specific overrides

## 2. Architecture-Aware Configuration Rules

- Bronze is the only local storage layer
- Silver and Gold live only in ClickHouse
- `fotmob.storage.bronze_path` is the only layer path in `config.yaml`
- Do not add `silver_path` or `gold_path` unless the architecture changes intentionally

## 3. `config.yaml`

`config.yaml` contains non-secret runtime settings.

Example:

```yaml
logging:
  level: INFO
  file: logs/scraper.log
  dir: logs

fotmob:
  api:
    base_url: https://www.fotmob.com/api/data
    user_agent: Mozilla/5.0 ...
    user_agents:
      - "Mozilla/5.0 ..."
  request:
    timeout: 30
    delay_min: 2.0
    delay_max: 4.0
  scraping:
    max_workers: 2
    enable_parallel: true
    metrics_update_interval: 20
    filter_by_status: true
    allowed_match_statuses:
      - Finished
      - FT
  storage:
    bronze_path: data/fotmob
    enabled: true
  retry:
    max_attempts: 3
    initial_wait: 2.0
    max_wait: 10.0
```

### Important `config.yaml` sections

#### `fotmob.api`

- Controls API base URL and browser-like headers
- `x_mas` token is not stored here; it belongs in `.env`

#### `fotmob.request`

- Controls request timeout and pacing
- Useful when tuning reliability versus scrape speed

#### `fotmob.scraping`

- Controls worker count, status filtering, and caching
- In runtime, the orchestrator may still force safer sequential scraping if needed

#### `fotmob.storage`

- `bronze_path` defines where raw Bronze files are stored
- This directory is used by the scraper and by Bronze-to-ClickHouse loading

## 4. `.env`

`.env` contains secrets and deployment-specific values.

Minimum recommended example:

```bash
FOTMOB_X_MAS_TOKEN=your_token_here

CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=fotmob_user
CLICKHOUSE_PASSWORD=fotmob_pass
CLICKHOUSE_DB_FOTMOB=fotmob

LOG_LEVEL=INFO
```

Optional useful values:

```bash
FOTMOB_BRONZE_PATH=data/fotmob
FOTMOB_REQUEST_TIMEOUT=30
FOTMOB_DELAY_MIN=2.0
FOTMOB_DELAY_MAX=4.0
FOTMOB_MAX_WORKERS=2
FOTMOB_ENABLE_PARALLEL=false
CONFIG_FILE_PATH=config.yaml
```

## 5. How Configuration Loads

`FotMobConfig()` loads settings in this order:

1. `config.yaml`
2. `.env` overrides
3. directory initialization for local Bronze storage and logging

That means `.env` wins over `config.yaml` for the values it overrides.

## 6. How To Run The Code

### Docker workflow

#### Step 1: start containers

```bash
docker-compose -f docker/docker-compose.yml up -d
```

#### Step 2: create ClickHouse schema

All layers:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/setup_clickhouse.py
```

Only Bronze:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/setup_clickhouse_bronze.py
```

Only Silver:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/setup_clickhouse_silver.py
```

Only Gold:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/setup_clickhouse_gold.py
```

#### Step 3: scrape raw Bronze files

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/scrape_fotmob.py 20251208
```

#### Step 4: load Bronze files into ClickHouse Bronze tables

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/load_clickhouse.py --scraper fotmob --date 20251208
```

#### Step 5: build Silver views

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/process_silver.py --date 20251208
```

#### Step 6: build Gold tables

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/process_gold.py --date 20251208
```

### Full orchestration

Single day:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208
```

Date range:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py --start-date 20251201 --end-date 20251207
```

Month:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py --month 202512
```

## 7. Pipeline Flags

### `--bronze-only`

Runs only the raw FotMob scrape into local Bronze storage.

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208 --bronze-only
```

### `--silver-only`

Runs only the Silver stage in ClickHouse.

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208 --silver-only
```

### `--gold-only`

Runs only the Gold stage in ClickHouse.

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208 --gold-only
```

### `--skip-bronze`

Skips scraping and reuses already-saved Bronze files.

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208 --skip-bronze
```

### `--force`

Forces reprocessing where supported.

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208 --force
```

## 8. Warehouse Naming Standards

These are mandatory for ClickHouse objects used by Scout.

### Bronze

- `fotmob.bronze_general`
- `fotmob.bronze_timeline`
- `fotmob.bronze_venue`
- `fotmob.bronze_player`
- `fotmob.bronze_shotmap`
- `fotmob.bronze_goal`
- `fotmob.bronze_cards`
- `fotmob.bronze_red_card`
- `fotmob.bronze_period`
- `fotmob.bronze_momentum`
- `fotmob.bronze_starters`
- `fotmob.bronze_substitutes`
- `fotmob.bronze_coaches`
- `fotmob.bronze_team_form`

### Silver

- `fotmob.silver_general`
- `fotmob.silver_player`
- `fotmob.silver_shotmap`
- `fotmob.silver_period`
- `fotmob.silver_venue`

### Gold

- `fotmob.gold_player_match_stats`
- `fotmob.gold_match_summary`
- `fotmob.gold_team_season_stats`

Bare warehouse names like `general`, `player`, or `timeline` should not be used for persisted ClickHouse objects.

## 9. Bronze Engine Standard

All Bronze tables use:

```sql
ENGINE = ReplacingMergeTree(inserted_at)
```

That means:

- re-runs are safe
- duplicates can be compacted later
- `inserted_at` is required on each Bronze table

Optimization command:

```bash
docker-compose -f docker/docker-compose.yml exec -T clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass \
  < clickhouse/bronze/02_optimize.sql
```

## 10. Validation And Health Checks

Check health:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/health_check.py
docker-compose -f docker/docker-compose.yml exec scraper python scripts/health_check.py --json
```

## 11. Troubleshooting

### `config.yaml` not found

- confirm the file exists in the project root
- confirm `CONFIG_FILE_PATH` is correct if you override it

### `.env` override not applied

- use the exact supported variable name
- restart the container or shell session after changing environment values

### No Bronze files found

- confirm `fotmob.storage.bronze_path` exists
- run `scripts/ensure_directories.py` if needed
- run `scripts/scrape_fotmob.py` before `scripts/load_clickhouse.py`

### ClickHouse schema missing

- run `scripts/setup_clickhouse.py`
- or run the layer-specific setup scripts in Bronze, Silver, Gold order

## 12. Scope Reminder

Scout is currently FotMob-only.

- Additional sources should not be added to the active configuration model unless the architecture changes deliberately
- Source-specific runtime commands should not be mixed into the FotMob-only docs without an explicit design update
