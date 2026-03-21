# Scout — Development Guide & Code Review

> **Senior Review — Feb 2026**
> Written as a combined developer reference and candid engineering assessment.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Flow](#data-flow)
3. [Storage Layers](#storage-layers)
4. [Validation System](#validation-system)
5. [Automatic Compression](#automatic-compression)
6. [ClickHouse Optimization](#clickhouse-optimization)
7. [Logging Improvements](#logging-improvements)
8. [Extension Points](#extension-points)
9. [Testing](#testing)
10. [Performance Metrics](#performance-metrics)
11. [Security Considerations](#security-considerations)
12. [Configuration Reference](#configuration-reference)
13. [Common Commands](#common-commands)
14. [Code Review — Current Status](#code-review--current-status)
15. [Technical Debt & Bugs](#technical-debt--bugs)
16. [Future Improvements](#future-improvements)
17. [Professional Recommendations](#professional-recommendations)

---

## Architecture Overview

### System Diagram

```
FotMob REST API
       │
       ▼
 playwright_fetcher
 daily_scraper.py
 match_scraper.py
       │
       ▼
          Bronze Layer (JSON → GZIP → TAR)
          data/fotmob/matches/YYYYMMDD/
                  │
                  ▼
        ClickHouse (Analytics DB)
        fotmob.*  (14 tables)
                  │
                  ▼
              S3 Backup (Arvan Cloud)
```

---

## Data Flow

```
1. Fetch Daily Listing  → API/Web
2. Get Match IDs        → Filter already scraped
3. Scrape Matches       → Parallel/Sequential
4. Save to Bronze       → Atomic writes + metadata
5. Compress             → GZIP + TAR (60-75% savings)
6. Load to ClickHouse   → Batch insert via DataFrames
7. Optimize Tables      → Manual DEDUPLICATE (make optimize-tables)
8. S3 Backup            → Optional: bronze/{scraper}/YYYYMM/YYYYMMDD.tar.gz
```

---

## Storage Layers

**Bronze Layer:**
```
data/
└── fotmob/
    ├── matches/YYYYMMDD/
    │   └── YYYYMMDD_matches.tar        (compressed archive)
    ├── lineage/YYYYMMDD/
    │   └── lineage.json
    └── daily_listings/YYYYMMDD/
        └── matches.json
```

**ClickHouse Schema:**
- **fotmob** database: 14 tables (general, player, shotmap, goal, cards, red_card, venue, timeline, period, momentum, starters, substitutes, coaches, team_form)

---

## Validation System

### Components

**1. SafeFieldExtractor** (`src/utils/fotmob_validator.py`)

Provides null-safe field extraction from nested dictionaries.

```python
from src.utils.fotmob_validator import SafeFieldExtractor

extractor = SafeFieldExtractor()
match_id = extractor.safe_get(data, 'general.matchId', default=0)
```

**2. FotMobValidator** (`src/utils/fotmob_validator.py`)

Validates API responses against expected schema.

```python
from src.utils.fotmob_validator import FotMobValidator

validator = FotMobValidator()
is_valid, errors, warnings = validator.validate_response(data)
```

**3. ResponseSaver** (`src/utils/fotmob_validator.py`)

Saves validated responses to organized JSON files.

---

## Automatic Compression

After scraping completes, the system automatically compresses match files into TAR archives, achieving 60-75% space savings.

**Process Flow:**
```
JSON Files → GZIP (.json.gz) → TAR Archive → Delete .json.gz → Keep .tar
```

**Statistics:**
- Before: 450 files, ~22 MB
- After: 1 TAR file, ~6.2 MB
- Savings: 15.8 MB (72%)

**Manual Compression:**
```python
from src.storage import BronzeStorage

bronze = BronzeStorage("data/fotmob")
stats = bronze.compress_date_files("20251209")
```

---

## ClickHouse Optimization

```bash
# Optimize all tables (recommended after bulk loading)
make optimize-tables
```

**When to Optimize:**
- After bulk data loading
- Periodically (daily, weekly, or monthly)
- When query performance degrades

---

## Logging Improvements

- **DEBUG**: Element interactions, verification steps
- **INFO**: Milestones, progress (every 10 matches)
- **WARNING**: Recoverable issues, low match counts
- **ERROR**: Critical failures

---

## Extension Points

### Adding New Scrapers

1. Create scraper module in `src/scrapers/{scraper_name}/`
2. Implement base scraper interface from `src/core/interfaces.py`
3. Add configuration in `config/`
4. Update `scripts/pipeline.py`
5. Create ClickHouse schema in `clickhouse/init/`

---

## Testing

```bash
# Run validation test suite
python scripts/test_validation.py

# Validate existing data
python scripts/validate_fotmob_responses.py data/fotmob/matches/20251208

# Health check
docker-compose -f docker/docker-compose.yml exec scraper python scripts/health_check.py
```

---

## Performance Metrics

| Operation | Time | Notes |
|-----------|------|-------|
| FotMob API scrape | ~0.5-1 sec/match | |
| Compression (100 matches) | ~1-2 sec | Byte-copy method |
| ClickHouse batch insert | 1000-5000 rows/batch | |
| Table optimization | 5-30 sec/table | |

---

## Security Considerations

**SQL Injection Protection:**
- Table name whitelist in ClickHouseClient
- Database parameter validation with regex
- Parameterized queries

**File System Security:**
- Atomic writes (temp → rename)
- File locking for concurrent access
- Path validation

**Credentials Management:**
- Sensitive data in `.env` file (not tracked in git)
- Application settings in `config.yaml`

---

## Configuration Reference

**Two-layer configuration:**
1. `config.yaml` — All application settings
2. `.env` — Secrets and environment-specific values

---

## Common Commands

```bash
# Full pipeline
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208

# Bronze only
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208 --bronze-only

# Optimize tables
make optimize-tables

# Health check
docker-compose -f docker/docker-compose.yml exec scraper python scripts/health_check.py
```

---

## Code Review — Current Status

### What's Working Well ✅

- **Atomic file writes with locking** — Production-grade concurrent safety
- **Bronze → ClickHouse separation** — Two-stage pipeline means raw data never lost
- **TAR compression pipeline** — 60-75% space savings with transparent decompression
- **ClickHouse table design** — ReplacingMergeTree with correct deduplication
- **League filtering** — 95 competition allowlist prevents bloat
- **Pydantic models** — v2 for response validation
- **Separation of concerns** — Clean split in scrapers
- **BaseBronzeStorage abstraction** — Avoids duplication

### Fixed Issues ✅

| Issue | Status |
|-------|--------|
| Race condition in `mark_match_as_scraped` | ✅ Fixed (FileLock added) |
| SQL injection in database parameter | ✅ Fixed (regex validation) |
| Compression inefficiency | ✅ Fixed (byte-copy method) |
| AlertManager thread-safety | ✅ Fixed (double-checked locking) |
| Logging pattern inconsistent | ✅ Fixed (logging.getLogger(__name__)) |
| Redundant imports inside methods | ✅ Fixed |
| Dead parameters in BaseScraper | ✅ Fixed |
| AlertChannel ABC declaration | ✅ Already implemented |

---

## Technical Debt & Bugs

### Bugs Still Pending

| # | Issue | Impact | Priority |
|---|-------|--------|----------|
| 1 | `asyncio.run()` in sync context | Will break in async context | Medium |
| 2 | O(n²) marking performance | 300 matches = 900 file I/O ops | Medium |
| 3 | Subprocess pipeline | No exception propagation, slow startup | Low |

### Technical Debt

| Item | Impact |
|------|--------|
| Code clarity issue | Maintainability |
| No pytest test suite | Reliability |
| `fotmob_credentials.py` hardcoded secrets | Security risk |
| Duplicate BronzeStorage classes | Confusion |

---

## Future Improvements

### High-Value Features

| Feature | Priority |
|---------|----------|
| Add more data sources (FlashScore, SofaScore) | High |
| Real-time/WebSocket scraping | High |
| Historical data backfill | Medium |
| REST API layer (FastAPI) | Medium |
| Prometheus + Grafana monitoring | Medium |

### Architecture Improvements

1. **Unit test suite** — Add pytest with core logic tests
2. **Refactor subprocess pipeline** — Direct function calls instead of subprocess
3. **Async pipeline conversion** — asyncio for better performance
4. **Batch marking** — Eliminate O(n²) pattern

---

## Professional Recommendations

As a senior data engineer, here are strategic recommendations for productionizing this system:

### 1. Data Quality Foundation

**Implement Great Expectations for automated data validation:**
- Schema enforcement on ingestion
- Completeness checks (missing matches, null fields)
- Anomaly detection (unusual score ranges, match durations)

```python
# Example: Define expectations
expect_column_values_to_be_between("home_score", 0, 20)
expect_column_values_to_not_be_null("match_id")
```

### 2. Schema Evolution Strategy

Your ClickHouse tables will evolve as FotMob changes their API. Implement:
- Versioned schemas with migration scripts
- Backward-compatible columns (always add, rarely remove)
- Graceful degradation for missing fields

### 3. Cost Optimization

| Area | Recommendation |
|------|----------------|
| **Storage** | Implement tiered storage: hot (recent 30 days) → cold (S3) |
| **Compute** | Use ClickHouse materialized views for pre-aggregations |
| **Scraping** | Add caching layer for repeated requests |

### 4. Observability Stack

```
┌─────────────────────────────────────────────────────────┐
│                    Scout Pipeline                        │
└─────────────────┬───────────────────────────────────────┘
                  │
        ┌─────────▼─────────┐
        │  Prometheus       │  ← Scrape metrics
        │  (metrics)        │
        └─────────┬─────────┘
                  │
        ┌─────────▼─────────┐
        │  Grafana          │  ← Dashboards
        │  (visualization)  │
        └─────────┬─────────┘
                  │
        ┌─────────▼─────────┐
        │  AlertManager    │  ← On-call
        │  (notifications) │
        └─────────────────┘
```

**Key metrics to track:**
- Scraping success rate (target: >95%)
- Average scrape time per match
- ClickHouse insert latency
- Data freshness (time from match end to available)
- Error rate by type

### 5. Incremental Loading Strategy

Currently, the pipeline reloads all data. For production:

```sql
-- Instead of: INSERT INTO ... SELECT * FROM bronze
-- Use: INSERT INTO ... SELECT * FROM bronze WHERE scraped_at > last_successful_load
```

This requires:
- Adding `scraped_at` timestamp column
- Tracking `last_successful_load` in metadata table
- Handling late-arriving data (matches updated after initial scrape)

### 6. Data Lake Considerations

For analytics at scale, consider:

| Option | Pros | Cons |
|--------|------|------|
| **Current (TAR)** | Simple, works | No partitioning |
| **Delta Lake** | ACID, time travel | Extra dependency |
| **Iceberg** | Cloud-native, efficient | Complexity |
| **Parquet directly** | Fast queries | No transactions |

**Recommendation:** Start with Parquet, migrate to Iceberg if needed.

### 7. Backfill Strategy

For historical data:

```python
# Priority order for backfill:
# 1. High-value leagues (Premier League, La Liga, etc.)
# 2. Recent seasons (2023-2025)
# 3. All other leagues

# Implement exponential backoff:
# - First attempt: immediate
# - Second: 1 min
# - Third: 5 min
# - Fourth: 30 min
# - Fifth+: 2 hours
```

### 8. Runbook Template

Document for operations:

```
## Incident: Low Scrape Success Rate

### Detection
- Prometheus alert: success_rate < 90%

### Diagnosis
1. Check logs: `grep ERROR logs/pipeline_*.log | tail -50`
2. Verify FotMob API: curl https://www.fotmob.com/api/matches
3. Check credentials: python scripts/refresh_turnstile.py --verify

### Mitigation
1. If 5xx errors: Wait and retry (FotMob may be down)
2. If auth errors: Run refresh_turnstile.py
3. If rate limited: Increase delay in config.yaml

### Recovery
1. Re-run failed dates: python scripts/pipeline.py 20251208 --force
2. Verify: python scripts/health_check.py
```

---

## Overall Assessment

| Dimension | Score | Notes |
|-----------|-------|-------|
| **Readability** | 8/10 | Good naming, docstrings, type hints |
| **Maintainability** | 8/10 | Clean architecture, some dead code remains |
| **Performance** | 7/10 | Good baseline, room for async optimization |
| **Security** | 8/10 | Fixed SQL injection, credentials need attention |
| **Testability** | 5/10 | No unit tests yet, but infrastructure is testable |

---

**Scout Development Guide** — Technical reference for developers and operators
