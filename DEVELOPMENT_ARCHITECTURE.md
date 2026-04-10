# Development and Architecture Guide

This document consolidates the former project docs:
- DEVELOPMENT.md
- CONFIG_GUIDE.md
- ARCHITECTURE_SUGGESTIONS.md
- src/core/README.md

---

## Part 1: Development Guide


This document defines the engineering standard for Bronze, Silver, and Gold layers in Scout, with focus on professional SQL organization and Python execution patterns for Silver and Gold.

## 1. Architecture Overview

### Medallion Flow

1. Bronze: raw ingestion and normalization
2. Silver: cleaned, conformed, reusable analytical entities
3. Gold: business-level aggregates and serving tables

Current pipeline entrypoint: `scripts/orchestration/pipeline.py`

Current schema setup runners:
- `scripts/bronze/setup_clickhouse.py`
- `scripts/silver/setup_clickhouse.py`
- `scripts/gold/setup_clickhouse_gold.py`

Current layer runners:
- `scripts/silver/load_clickhouse.py`
- `scripts/gold/load_clickhouse_scenarios.py`

Current shared SQL execution helper:
- `src/storage/clickhouse_sql_executor.py`

## 2. Layer Responsibility Boundaries

### Bronze
- Ingest raw/semi-structured source data
- Preserve source fidelity and traceability
- Minimal transformation

### Silver
- Clean and standardize fields
- Resolve data types, null handling, key consistency
- Build stable entities/views for downstream use

### Gold
- Build aggregate and domain-ready metrics
- Optimize for BI/reporting/product use cases
- Keep business logic explicit and testable

## 3. SQL Folder Structure (Current + Target)

Current repository structure:

```text
clickhouse/
  silver/
    create/
      00_create_database.sql
      01_match.sql
      ...
    load/
      01_match.sql
      02_period_stat.sql
      ...
  gold/
    00_create_database.sql
    01_create_scenario_tables.sql
    scenario/
      scenario_*.sql
```

Target structure (future refactor):
- move toward `ddl/` + `dml/` folders for both Silver and Gold
- keep behavior unchanged during migration

### Naming Standard

- Silver create/load format: `NN_<entity>.sql`
- Gold scenario format: `scenario_<name>.sql`
- Numeric prefixes control order; lexical sort is execution order
- Keep one concern per file (avoid very large mixed scripts)

### SQL Authoring Rules

- Prefer idempotent statements:
  - `CREATE TABLE IF NOT EXISTS`
  - `CREATE OR REPLACE VIEW`
- Keep schema creation SQL separate from data load SQL
- Keep SQL deterministic and re-runnable
- Use comments for business intent, not obvious syntax

## 4. Python Runner Standard (Silver/Gold)

Python is orchestration, SQL is transformation logic.

### Runner Responsibilities

- Discover SQL files by layer/stage (`create`, `load`, `scenario`)
- Execute in deterministic order
- Log query file, statement count, elapsed time, success/failure
- Stop on failure with clear context

### Runner Must Not

- Embed large SQL business logic inside Python strings
- Build unsafe dynamic SQL from raw user input
- Mix orchestration concerns with transformation definitions

### Dynamic Query Policy

Default policy is static SQL files.

If runtime behavior is needed (date/month/window):
1. Prefer pre-defined SQL variants in separate files
2. If dynamic injection is unavoidable, only allow strict allowlisted values in Python before execution
3. Never directly concatenate unsanitized input into SQL

## 5. Recommended Code Structure in Current Repo

Keep your existing shape and evolve it incrementally.

### Keep

- `src/processors/silver/fotmob.py` and `src/processors/gold/fotmob.py` for SQL discovery
- `src/storage/silver/fotmob.py` and `src/storage/gold/fotmob.py` for execution wiring
- `src/storage/clickhouse_sql_executor.py` for shared execution logic

### Improve Next

1. Migrate folder conventions from `create/load` to `ddl/dml` without changing runtime behavior
2. Add optional `--dry-run` in `scripts/silver/load_clickhouse.py` and `scripts/gold/load_clickhouse_scenarios.py`
3. Add execution summary object (files run, statements run, elapsed seconds, failed file)
4. Add query-level metrics/logging for better observability

## 6. How to Add a New Silver Query

1. Identify query type:
- Schema/table change -> `clickhouse/silver/create/`
- Data refresh/load -> `clickhouse/silver/load/`

2. Add file with next ordered prefix:
- Example: `09_player_quality_rules.sql`

3. Ensure idempotency and safe rerun behavior

4. Run locally (Python scripts directly; no Makefile required):
- `python scripts/silver/setup_clickhouse.py` (only if `create/` changed)
- `python scripts/silver/load_clickhouse.py` (data load only)

5. Validate outputs with explicit checks in ClickHouse

6. Add or update tests if transformation behavior changed

## 7. How to Add a New Gold Query

1. Decide object type:
- Schema/table change -> top-level `clickhouse/gold/*.sql`
- Scenario logic -> `clickhouse/gold/scenario/`

2. Add ordered SQL file:
- Example: `130_refresh_team_form.sql`

3. Ensure rerun strategy is explicit:
- Full refresh, partition refresh, or incremental append

4. Run locally:
- `python scripts/gold/load_clickhouse_scenarios.py`

5. Validate row counts, keys, and metric sanity

## 8. Testing and Validation Standard

### Minimum Automated Coverage

- SQL discovery order tests
- SQL splitting and statement execution tests
- Runner failure behavior tests (fail-fast with file context)
- CLI argument validation tests for pipeline entrypoints

### Data Quality Checks

- Null rate checks on critical columns
- Duplicate key checks on expected unique keys
- Freshness checks for latest processed dates
- Aggregate reconciliation checks (Silver vs Gold)

## 9. Operational Runbook

### Core Commands

```bash
# One-time (or when schema SQL changes)
python scripts/orchestration/setup_clickhouse.py

# Full pipeline
python scripts/orchestration/pipeline.py 20251113

# Silver only
python scripts/orchestration/pipeline.py 20251113 --silver-only

# Gold only
python scripts/orchestration/pipeline.py 20251113 --gold-only

# Direct layer runs
python scripts/silver/load_clickhouse.py
python scripts/gold/load_clickhouse_scenarios.py
```

### Incident Basics

1. Identify failing SQL file from logs
2. Re-run layer for same date/month after fix
3. Validate target tables/views with row count and sample checks
4. Record root cause and preventive action

## 10. Prioritized Engineering Backlog (Non-Redundant)

### P0

- Keep `setup_clickhouse` as the only place for schema creation
- Keep `load_clickhouse` scripts data-only for Silver and Gold
- Standardize SQL naming and ordering convention
- Keep transformation logic in SQL files, not embedded Python

### P1

- Add `--dry-run` support for Silver/Gold runners
- Add query execution summaries and consistent structured logs
- Add tests around SQL discovery and failure semantics

### P2

- Add lightweight data contracts and quality assertions per layer
- Add incremental refresh strategy per Gold table
- Add layer-level performance dashboards (duration, success rate)

## 11. Final Standards Checklist

Use this checklist for each PR touching Silver/Gold:

- Query file is in correct layer and stage (`create`, `load`, or `scenario`)
- Naming follows the active layer convention (`NN_<entity>.sql` or `scenario_<name>.sql`)
- SQL is idempotent or rerun strategy is explicit
- Python changes are orchestration-only
- Tests updated for behavior changes
- Documentation updated when structure changes

## 12. Future Working Plan

Use this as the default roadmap for the next iterations.

### Phase 1 (Now)

1. Keep boundaries strict:
- `setup_clickhouse` scripts create/alter schema objects
- `load_clickhouse` scripts insert/refresh data only
2. Add a CI check that fails if `scripts/silver/load_clickhouse.py` imports or executes `clickhouse/silver/create/*.sql`
3. Add a short runbook section in PR templates: setup first, then load

### Phase 2 (Next)

1. Add `--dry-run` to Silver and Gold loaders
2. Emit execution summary per run (files, statements, duration, failed file)
3. Add data quality checks for key Silver tables (nulls, duplicates, freshness)

### Phase 3 (Later)

1. Migrate folder naming from `create/load` to `ddl/dml` gradually
2. Add backward-compatible loader discovery during migration
3. Remove legacy path support only after all SQL files and scripts are migrated

---

This guide is now the single source of truth for Silver/Gold development workflow in Scout.

---

## Part 2: Configuration Guide


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
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/setup_clickhouse.py
```

Only Bronze:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/bronze/setup_clickhouse.py
```

Only Silver:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/silver/setup_clickhouse.py
```

Only Gold:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/gold/setup_clickhouse_gold.py
```

#### Step 3: scrape raw Bronze files

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/bronze/scrape_fotmob.py 20251208
```

#### Step 4: load Bronze files into ClickHouse Bronze tables

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/bronze/load_clickhouse.py --date 20251208
```

#### Step 5: build Silver tables

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/silver/load_clickhouse.py
```

#### Step 6: build Gold tables

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/gold/load_clickhouse_scenarios.py
```

### Full orchestration

Single day:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208
```

Date range:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py --start-date 20251201 --end-date 20251207
```

Month:

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py --month 202512
```

## 7. Pipeline Flags

### `--bronze-only`

Runs only the raw FotMob scrape into local Bronze storage.

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208 --bronze-only
```

### `--silver-only`

Runs only the Silver stage in ClickHouse.

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208 --silver-only
```

### `--gold-only`

Runs only the Gold stage in ClickHouse.

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208 --gold-only
```

### `--skip-bronze`

Skips scraping and reuses already-saved Bronze files.

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208 --skip-bronze
```

### `--force`

Forces reprocessing where supported.

```bash
docker-compose -f docker/docker-compose.yml exec scraper python scripts/orchestration/pipeline.py 20251208 --force
```

## 8. Warehouse Naming Standards

These are mandatory for ClickHouse objects used by Scout.

### Bronze

- `bronze.general`
- `bronze.timeline`
- `bronze.venue`
- `bronze.player`
- `bronze.shotmap`
- `bronze.goal`
- `bronze.cards`
- `bronze.red_card`
- `bronze.period`
- `bronze.momentum`
- `bronze.starters`
- `bronze.substitutes`
- `bronze.coaches`
- `bronze.team_form`

### Silver

- `silver.general`
- `silver.player`
- `silver.shotmap`
- `silver.period`
- `silver.venue`

### Gold

- `gold.player_match_stats`
- `gold.match_summary`
- `gold.team_season_stats`

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
  < clickhouse/bronze/99_optimize_tables.sql
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
- run `scripts/bronze/scrape_fotmob.py` before `scripts/bronze/load_clickhouse.py`

### ClickHouse schema missing

- run `scripts/orchestration/setup_clickhouse.py`
- or run the layer-specific setup scripts in Bronze, Silver, Gold order

## 12. Scope Reminder

Scout is currently FotMob-only.

- Additional sources should not be added to the active configuration model unless the architecture changes deliberately
- Source-specific runtime commands should not be mixed into the FotMob-only docs without an explicit design update

---

## Part 3: Architecture Suggestions


> Updated: 2026-03-25
> Scope: FotMob-only medallion pipeline

This document reflects the current target architecture for Scout after the layer-separation cleanup.

## 1. Product Scope

Scout is now a FotMob-only pipeline.

- All scraping, raw storage, transformations, and warehouse objects are defined around FotMob only
- Any future source should be added as a separate, explicit design decision rather than mixed into the current flow

## 2. Layer Model

### Bronze

Bronze is the raw ingestion layer.

- Input: raw FotMob API responses
- Storage: local filesystem under `data/fotmob/`
- Backup: optional S3 archive upload
- ClickHouse representation: `bronze.*` tables
- Responsibility:
  `scripts/bronze/scrape_fotmob.py`
  `scripts/bronze/load_clickhouse.py`
  `src/storage/bronze/`
  `src/processors/bronze/`

### Silver

Silver is the cleaned relational layer inside ClickHouse.

- Input: ClickHouse Bronze tables
- Storage: ClickHouse only
- ClickHouse representation: `silver.*` tables
- Responsibility:
  `scripts/silver/load_clickhouse.py`
  `src/storage/silver/`
  `src/processors/silver/`

### Gold

Gold is the analytics-ready aggregation layer inside ClickHouse.

- Input: ClickHouse Silver tables
- Storage: ClickHouse only
- ClickHouse representation: `gold.*` tables
- Responsibility:
  `scripts/gold/load_clickhouse_scenarios.py`
  `src/storage/gold/`
  `src/processors/gold/`

## 3. Naming Rules

These rules should be treated as required, not optional.

- Bronze ClickHouse tables must be created in the `bronze` schema
- Silver ClickHouse tables must be created in the `silver` schema
- Gold ClickHouse tables must be created in the `gold` schema
- No new warehouse object should use unqualified names like `general`, `player`, or `timeline`
- Bare logical names are acceptable only as internal Python mapping keys before applying a schema qualifier
- Python entry points should use explicit layer-aware names where possible, for example `FotMobBronzeStorage` and `FotMobBronzeMatchProcessor`

## 4. Storage Rules

- Only Bronze has a local storage path in configuration
- Silver and Gold are not filesystem layers in this project
- `config.yaml` should expose `fotmob.storage.bronze_path` only
- Any new `silver_path` or `gold_path` setting should be rejected unless the architecture changes intentionally

## 5. ClickHouse Rules

### Bronze

- All Bronze tables must use `ReplacingMergeTree(inserted_at)`
- All Bronze tables must include an `inserted_at DateTime DEFAULT now()` column
- Bronze deduplication is operationally completed with `OPTIMIZE TABLE ... FINAL DEDUPLICATE`
- Bronze tables should remain append-friendly and re-runnable

### Silver

- Silver objects should remain deterministic transformations from Bronze
- Silver objects should be created in the `silver` schema unless there is a deliberate reason to use a different schema

### Gold

- Gold objects should be explicitly business-facing or analytics-facing aggregates
- Gold objects must read from `silver.*`, not directly from raw Bronze tables

## 6. Script Boundaries

Scripts should map to one responsibility each.

- `scripts/bronze/scrape_fotmob.py`: scrape raw FotMob data into Bronze filesystem storage
- `scripts/bronze/load_clickhouse.py`: parse Bronze files and insert into ClickHouse Bronze tables
- `scripts/silver/load_clickhouse.py`: load Silver data into existing `silver.*` tables only (no schema creation)
- `scripts/gold/load_clickhouse_scenarios.py`: create or refresh Gold tables and scenario narrative tables
- `scripts/bronze/setup_clickhouse.py`: create Bronze schema only
- `scripts/silver/setup_clickhouse.py`: create Silver schema only
- `scripts/gold/setup_clickhouse_gold.py`: create Gold schema only
- `scripts/orchestration/setup_clickhouse.py`: convenience wrapper that runs the three layer setup scripts in order
- `scripts/orchestration/pipeline.py`: orchestration wrapper, not a place to hide layer-specific logic

## 7. Recommended End-to-End Flow

```text
FotMob API
  -> local Bronze files
  -> ClickHouse Bronze tables
  -> ClickHouse Silver tables
  -> ClickHouse Gold tables
```

Operational order:

1. Create schema
2. Scrape FotMob raw data into Bronze files
3. Load Bronze files into ClickHouse Bronze tables
4. Build Silver tables
5. Build Gold tables
6. Run Bronze table optimization when needed

## 8. Guardrails For Future Changes

- Do not reintroduce mixed-source abstractions unless the repo truly becomes multi-source again
- Do not add generic warehouse table names without a layer schema qualifier
- Do not let Silver or Gold depend on local filesystem artifacts
- Do not bypass Bronze and load raw scraper responses directly into Silver or Gold
- Do not add temporary references to other data sources back into docs or scripts

## 9. Current Cleanup Status

The architecture is now aligned with these rules in the main runtime paths:

- FotMob-only scope is enforced in docs and active scripts
- Bronze config is the only filesystem-backed layer config
- Bronze setup, Silver setup, and Gold setup have separate script entry points
- Bronze warehouse tables are stored in `bronze.*` and use `ReplacingMergeTree(inserted_at)`
- Silver and Gold warehouse objects are stored in `silver.*` and `gold.*`

Any future changes should preserve this contract.

---

## Part 4: Core Package Reference


The `core` package provides foundational components for the Scout project.

## Overview

This package contains interfaces, exceptions, constants, and type definitions that are used throughout the application. It establishes contracts and standards that all other modules should follow.

## Modules

### 📋 `interfaces.py`

Protocol definitions for all major components using Python's `Protocol` typing.

**Protocols Defined:**
- `StorageProtocol` - Interface for storage implementations (Bronze/Silver/Gold)
- `ScraperProtocol` - Interface for scraper implementations (FotMob)
- `ProcessorProtocol` - Interface for data processors
- `OrchestratorProtocol` - Interface for orchestration logic
- `ConfigProtocol` - Interface for configuration classes
- `CacheProtocol` - Interface for cache implementations
- `MetricsProtocol` - Interface for metrics tracking
- `LoggerProtocol` - Interface for logging

**Usage:**
```python
from src.core import StorageProtocol, ScraperProtocol

# Type hints with protocols
def process_data(storage: StorageProtocol, scraper: ScraperProtocol):
    match_data = scraper.fetch_match_details("12345")
    storage.save_match("12345", match_data, "20241218")
```

**Benefits:**
- Structural typing (duck typing with type safety)
- Easy to swap implementations for testing
- Clear contracts for components
- Better IDE autocomplete and type checking

---

### ⚠️ `exceptions.py`

Custom exception hierarchy for the entire application.

**Exception Hierarchy:**
```
ScoutError (base)
├── ConfigurationError
├── StorageError
│   ├── StorageReadError
│   ├── StorageWriteError
│   └── StorageNotFoundError
├── ScraperError
│   ├── ScraperConnectionError
│   ├── ScraperTimeoutError
│   ├── ScraperRateLimitError
│   └── ScraperParseError
├── ProcessorError
│   └── ValidationError
├── DatabaseError
│   ├── DatabaseConnectionError
│   └── DatabaseQueryError
└── OrchestratorError
```

**Usage:**
```python
from src.core import StorageError, ScraperError

try:
    storage.save_match(match_id, data, date)
except StorageWriteError as e:
    logger.error(f"Failed to save: {e}")
    logger.debug(f"Details: {e.to_dict()}")
except StorageError as e:
    logger.error(f"Storage error: {e}")
```

**Features:**
- Structured error information with `details` dict
- `to_dict()` method for serialization
- Rich context in error messages
- Easy error categorization

---

### 🔢 `constants.py`

Project-wide constants and enumerations.

**Categories:**
- Date and time formats
- HTTP status codes
- File extensions
- Match statuses
- Scraper names
- Storage layers
- Default values
- Environment variable names
- Regex patterns
- Table names
- Error messages

**Usage:**
```python
from src.core.constants import (
    MatchStatus,
    Defaults,
    HttpStatus,
    DATE_FORMAT_COMPACT,
)

# Use constants instead of magic values
if response.status_code == HttpStatus.RATE_LIMITED:
    time.sleep(Defaults.RETRY_INITIAL_WAIT)

if match_status in MatchStatus.COMPLETED_STATUSES:
    process_completed_match(match)
```

**Benefits:**
- No magic numbers or strings
- Single source of truth
- Easy to update values
- Improved code readability

---

### 📝 `types.py`

Common type definitions and type aliases.

**Type Categories:**
- Basic aliases (`MatchID`, `DateStr`, `URL`)
- Data structures (`JSONDict`, `Headers`)
- Status types (`MatchStatusType`, `ScrapeStatus`)
- Structured types (`TeamData`, `MatchMetadata`)
- Function return types
- Type validators

**Usage:**
```python
from src.core.types import (
    MatchID,
    DateStr,
    JSONDict,
    ScraperMetrics,
    is_valid_date_str,
)

def scrape_match(match_id: MatchID, date: DateStr) -> JSONDict:
    """Type-safe function signature."""
    if not is_valid_date_str(date):
        raise ValueError(f"Invalid date: {date}")
    ...

# Structured types with TypedDict
metrics: ScraperMetrics = {
    'total_matches': 100,
    'successful_matches': 95,
    'failed_matches': 5,
    'duration_seconds': 123.45,
}
```

**Benefits:**
- Better IDE autocomplete
- Type checking with mypy
- Clear function signatures
- Structured data validation

---

## Integration Examples

### Example 1: Implementing a New Storage

```python
from src.core import StorageProtocol
from src.core.exceptions import StorageError, StorageWriteError
from src.core.types import MatchID, DateStr, JSONDict
from pathlib import Path

class RedisStorage(StorageProtocol):
    """Redis-based storage implementation."""
    
    def save_match(
        self,
        match_id: MatchID,
        data: JSONDict,
        date: DateStr
    ) -> Path:
        try:
            key = f"match:{date}:{match_id}"
            self.redis.set(key, json.dumps(data))
            return Path(f"redis://{key}")
        except Exception as e:
            raise StorageWriteError(
                f"Failed to save match {match_id}",
                details={'date': date, 'error': str(e)}
            )
    
    def load_match(
        self,
        match_id: MatchID,
        date: Optional[DateStr] = None
    ) -> Optional[JSONDict]:
        # Implementation...
        pass
    
    def match_exists(
        self,
        match_id: MatchID,
        date: Optional[DateStr] = None
    ) -> bool:
        # Implementation...
        pass
```

### Example 2: Using Constants and Types

```python
from src.core.constants import MatchStatus, Defaults, HttpStatus
from src.core.types import MatchStatusType, ScraperMetrics
from src.core.exceptions import ScraperRateLimitError

def fetch_with_retry(url: str, max_retries: int = Defaults.MAX_RETRIES):
    """Fetch URL with automatic retry."""
    for attempt in range(max_retries):
        response = requests.get(url, timeout=Defaults.HTTP_TIMEOUT)
        
        if response.status_code == HttpStatus.RATE_LIMITED:
            raise ScraperRateLimitError(
                "Rate limit exceeded",
                details={'retry_after': response.headers.get('Retry-After')}
            )
        
        if response.status_code == HttpStatus.OK:
            return response.json()
    
    return None

def filter_completed_matches(matches: list) -> list:
    """Filter for completed matches only."""
    return [
        match for match in matches
        if match['status'] in MatchStatus.COMPLETED_STATUSES
    ]
```

### Example 3: Type-Safe Configuration

```python
from src.core import ConfigProtocol, ValidationError
from src.core.types import LogLevel, ValidationResult

class MyConfig(ConfigProtocol):
    """Custom configuration with validation."""
    
    def __init__(self):
        self.log_level: LogLevel = "INFO"
        self.timeout: int = 30
    
    def validate(self) -> ValidationResult:
        """Validate configuration values."""
        errors = []
        
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.log_level not in valid_levels:
            errors.append(f"Invalid log_level: {self.log_level}")
        
        if self.timeout <= 0:
            errors.append(f"timeout must be positive: {self.timeout}")
        
        return errors
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            'log_level': self.log_level,
            'timeout': self.timeout,
        }
```

---

## Testing with Protocols

Protocols make testing easier by allowing mock implementations:

```python
from src.core import StorageProtocol, ScraperProtocol
from src.core.types import MatchID, DateStr, JSONDict

class MockStorage(StorageProtocol):
    """Mock storage for testing."""
    
    def __init__(self):
        self.data = {}
    
    def save_match(self, match_id: MatchID, data: JSONDict, date: DateStr):
        self.data[match_id] = data
        return Path(f"/mock/{date}/{match_id}.json")
    
    def load_match(self, match_id: MatchID, date: DateStr = None):
        return self.data.get(match_id)
    
    def match_exists(self, match_id: MatchID, date: DateStr = None):
        return match_id in self.data

# Use in tests
def test_scraper():
    mock_storage = MockStorage()
    scraper = MyScraper(storage=mock_storage)
    
    scraper.scrape_match("12345", "20241218")
    assert mock_storage.match_exists("12345")
```

---

## Best Practices

1. **Always use protocols for type hints** instead of concrete classes
2. **Catch specific exceptions** instead of broad `Exception`
3. **Use constants** instead of hardcoded values
4. **Define type aliases** for complex types
5. **Validate data** using type guards from `types.py`
6. **Provide rich error context** using `details` parameter
7. **Document protocol implementations** with docstrings

---

## Migration Guide

### Before (Without Core):
```python
def save_match(match_id: str, data: dict, date: str) -> str:
    if response.status_code == 429:  # Magic number
        time.sleep(2)  # Magic number
        raise Exception("Rate limited")  # Generic exception
    
    if match_status == "Finished":  # Magic string
        process_match(data)
```

### After (With Core):
```python
from src.core import StorageProtocol, ScraperRateLimitError
from src.core.constants import HttpStatus, Defaults, MatchStatus
from src.core.types import MatchID, DateStr, JSONDict
from pathlib import Path

def save_match(
    match_id: MatchID,
    data: JSONDict,
    date: DateStr
) -> Path:
    if response.status_code == HttpStatus.RATE_LIMITED:
        time.sleep(Defaults.RETRY_INITIAL_WAIT)
        raise ScraperRateLimitError(
            "Rate limit exceeded",
            details={'match_id': match_id, 'date': date}
        )
    
    if match_status == MatchStatus.FINISHED:
        process_match(data)
```

---

## Contributing

When adding new components:

1. Define protocols in `interfaces.py` first
2. Add exceptions to appropriate category in `exceptions.py`
3. Add constants to appropriate section in `constants.py`
4. Define type aliases in `types.py` if needed
5. Update `__init__.py` exports
6. Update this README

---

## Version

Current version: **1.0.0**
