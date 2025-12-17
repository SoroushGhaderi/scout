# Scout - Development Guide

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Validation System](#validation-system)
3. [Automatic Compression](#automatic-compression)
4. [ClickHouse Optimization](#clickhouse-optimization)
5. [Logging Improvements](#logging-improvements)
6. [Extension Points](#extension-points)
7. [Testing](#testing)

---

## Architecture Overview

### Data Flow

```
1. Fetch Daily Listing → API/Web
2. Get Match IDs → Filter already scraped
3. Scrape Matches → Parallel/Sequential
4. Save to Bronze → Atomic writes + metadata
5. Compress → GZIP + TAR (60-75% savings)
6. Load to ClickHouse → Batch insert
7. Optimize Tables → Manual DEDUPLICATE
```

### Storage Layers

**Bronze Layer:**
```
data/
├── fotmob/
│   ├── matches/YYYYMMDD/
│   │   └── YYYYMMDD_matches.tar  (compressed archive)
│   ├── lineage/YYYYMMDD/
│   │   └── lineage.json
│   └── daily_listings/YYYYMMDD/
│       └── matches.json
└── aiscore/
    ├── matches/YYYYMMDD/
    │   └── YYYYMMDD_matches.tar  (compressed archive)
    ├── lineage/YYYYMMDD/
    │   └── lineage.json
    └── daily_listings/YYYYMMDD/
        └── matches.json
```

**ClickHouse Schema:**
- **fotmob** database: 14 tables (general, player, shotmap, goal, cards, red_card, venue, timeline, period, momentum, starters, substitutes, coaches, team_form)
- **aiscore** database: 5 tables (matches, odds_1x2, odds_asian_handicap, odds_over_under, daily_listings)

---

## Validation System

### Overview

Comprehensive validation for FotMob API responses with safe field extraction and automated response saving.

### Components

**1. SafeFieldExtractor (`src/utils/fotmob_validator.py`)**

Provides null-safe field extraction from nested dictionaries.

```python
from src.utils.fotmob_validator import SafeFieldExtractor

extractor = SafeFieldExtractor()

# Dot notation
match_id = extractor.safe_get(data, 'general.matchId', default=0)

# Nested keys
home_team = extractor.safe_get_nested(
    data, 'general', 'homeTeam', 'name', default='Unknown'
)
```

**2. FotMobValidator (`src/utils/fotmob_validator.py`)**

Validates API responses against expected schema.

```python
from src.utils.fotmob_validator import FotMobValidator

validator = FotMobValidator()
is_valid, errors, warnings = validator.validate_response(data)

# With reporting
validator.validate_and_report(data, match_id='12345')

# Detailed summary
summary = validator.get_validation_summary(data)
```

**Validation Rules:**

Required Fields:
- `general.matchId` (int/str)
- `general.homeTeam.id` (int)
- `general.homeTeam.name` (str)
- `general.awayTeam.id` (int)
- `general.awayTeam.name` (str)
- `header.status.finished` (bool)
- `header.status.started` (bool)

Finished Match Fields:
- `header.status.scoreStr` (str)
- `header.teams` (list)

Optional Fields (strict mode):
- `content.shotmap`
- `content.lineup`
- `content.playerStats`

**3. ResponseSaver (`src/utils/fotmob_validator.py`)**

Saves validated responses to organized JSON files.

```python
from src.utils.fotmob_validator import ResponseSaver

saver = ResponseSaver(output_dir='data/validated_responses')
saver.save_response(data, match_id='12345', validation_summary=summary)
```

### Usage

**Integrated in Match Processor:**

```python
from src.processors.match_processor import MatchProcessor

processor = MatchProcessor(save_responses=True)
dataframes, validation = processor.process_all(
    raw_data,
    validate_before_processing=True
)

if validation['is_valid']:
    # Load to ClickHouse
    pass
else:
    # Log errors
    print(f"Errors: {validation['errors']}")
```

**Standalone Validation:**

```bash
# Validate existing files
python scripts/validate_fotmob_responses.py data/fotmob/matches/20251208

# Generate Excel report
python scripts/validate_fotmob_responses.py data/fotmob/matches \
    --output-dir data/validation_reports
```

**Test Suite:**

```bash
python scripts/test_validation.py
```

---

## Automatic Compression

### Overview

After scraping completes, the system automatically compresses match files into TAR archives, achieving 60-75% space savings.

### Process Flow

```
JSON Files → GZIP (.json.gz) → TAR Archive → Delete .json.gz → Keep .tar
```

### Implementation

**AIScore (`scripts/scrape_aiscore.py`):**

```python
def process_single_date(args, date_str):
    # 1. Scrape links
    links_success = _process_links_step(args, date_str, matches_exist)

    # 2. Scrape odds
    odds_success = _process_odds_step(args, date_str)

    # 3. Automatic compression
    if odds_success and not args.links_only:
        _compress_date_files(date_str)
```

**FotMob (`src/orchestrator.py`):**

```python
def scrape_date(self, date_str, force_rescrape=False):
    # ... scraping logic ...

    # Automatic compression
    if self.bronze_only and metrics.successful_matches > 0:
        compression_stats = self.bronze_storage.compress_date_files(date_str)
        logger.info(
            f"Saved {compression_stats['saved_mb']} MB "
            f"({compression_stats['saved_pct']}% reduction)"
        )
```

### Reading Compressed Data

Both storage classes automatically handle reading from TAR archives:

```python
# Works with .tar, .json.gz, or .json
match_data = bronze_storage.load_raw_match_data(match_id, date_str)
```

**Reading Priority:**
1. Check for `.tar` archive
2. Extract `.json.gz` from archive
3. Decompress and return data
4. Fallback to individual files

### Statistics

Typical compression results:
- **Before**: 450 files, ~22 MB
- **After**: 1 TAR file, ~6.2 MB
- **Savings**: 15.8 MB (72%)

### Manual Compression

```python
from src.storage import BronzeStorage

bronze = BronzeStorage("data/fotmob")
stats = bronze.compress_date_files("20251209")
print(f"Saved {stats['saved_mb']:.2f} MB ({stats['saved_pct']:.0f}%)")
```

---

## ClickHouse Optimization

### Overview

Table optimization moved from Python to SQL for better separation of concerns. Operators control when optimization occurs.

### Changes

**Removed from Python (`scripts/load_clickhouse.py`):**
- `optimize_table()` function
- `optimize_all_tables()` function
- All automatic optimization calls

**Added SQL Script (`clickhouse/init/03_optimize_tables.sql`):**
```sql
OPTIMIZE TABLE fotmob.general FINAL DEDUPLICATE;
OPTIMIZE TABLE fotmob.timeline FINAL DEDUPLICATE;
-- ... all tables
OPTIMIZE TABLE aiscore.matches FINAL DEDUPLICATE;
-- ... all tables
```

**Added Makefile Target:**
```makefile
optimize-tables:
	docker-compose -f docker/docker-compose.yml exec -T clickhouse clickhouse-client \
	  --user fotmob_user --password fotmob_pass \
	  < clickhouse/init/03_optimize_tables.sql
```

### Usage

```bash
# Optimize all tables (recommended)
make optimize-tables

# Manual optimization
docker-compose -f docker/docker-compose.yml exec clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass \
  --query "OPTIMIZE TABLE fotmob.general FINAL DEDUPLICATE"
```

### When to Optimize

Run optimization:
- After bulk data loading
- Periodically (daily, weekly, or monthly)
- When query performance degrades
- After deleting/updating large amounts of data

### Benefits

1. **Separation of Concerns**: Data loading and optimization are separate
2. **Performance**: Load operations complete faster
3. **Flexibility**: Operators choose when to optimize
4. **Control**: Can optimize specific tables or databases

---

## Logging Improvements

### Overview

All logging improved to be concise, professional, and insightful without emojis or visual separators.

### Key Changes

**1. Removed Emojis:**
```python
# Before
logger.info(f"✓ Found {total_tabs} tabs")

# After
logger.info(f"Located {total_tabs} odds tabs")
```

**2. Removed Separators:**
```python
# Before
logger.info("=" * 60)
logger.info("Starting scraping...")
logger.info("=" * 60)

# After
logger.info("Starting scraping via scroll extraction")
```

**3. Improved Log Levels:**

- **DEBUG**: Element interactions, verification steps
- **INFO**: Milestones, progress (every 10 matches)
- **WARNING**: Recoverable issues, low match counts
- **ERROR**: Critical failures

**4. Enhanced Metrics:**
```python
# Before
logger.info(f"Scraped {count} matches")

# After
logger.info(
    f"Scraping complete: {success}/{total} successful ({success_rate:.1f}%), "
    f"{total_odds} odds extracted"
)
```

### Log Output Example

**Before:**
```
================================================================================
Football Scraper | Date: 20251209
================================================================================
[INFO] ✓ Cloudflare passed
[INFO] ✓ Found 3 tabs
[PROGRESS] 1/100 (1.0%)
================================================================================
```

**After:**
```
[INFO] Starting scraper for 20251209 (headless mode)
[DEBUG] Cloudflare check passed (3s)
[INFO] Located 3 odds tabs: 1 X 2, Asian Handicap, Over/Under
[INFO] Progress: 10/100 (10%) - Success: 10, Failed: 0
[INFO] Scraping complete: 95/100 successful (95.0%), 4500 odds extracted
```

### Benefits

1. Cleaner, easier to read
2. Better performance (reduced I/O)
3. More actionable information
4. Professional standard
5. Cross-platform compatible
6. Easier to grep/search
7. Metrics-driven summaries

---

## Extension Points

### Adding New Scrapers

1. Create scraper module in `src/scrapers/{scraper_name}/`
2. Implement base scraper interface
3. Add configuration in `src/config/{scraper_name}_config.py`
4. Update pipeline script
5. Create ClickHouse schema

```python
# Example structure
src/scrapers/new_scraper/
├── __init__.py
├── scraper.py          # Main scraper logic
├── bronze_storage.py   # Storage implementation
└── models.py           # Data models
```

### Adding New Data Sources

1. Extend BronzeStorage or create scraper-specific storage
2. Add data models in `src/models/`
3. Create processor if transformation needed
4. Update ClickHouse schema

### Custom Processing

1. Extend MatchProcessor or create new processor
2. Add validation rules in `src/utils/validation.py`
3. Integrate into orchestrator workflow

---

## Testing

### Validation Tests

```bash
# Run validation test suite
python scripts/test_validation.py

# Validate existing data
python scripts/validate_fotmob_responses.py data/fotmob/matches/20251208
```

### Health Checks

```bash
# System health check
docker-compose -f docker/docker-compose.yml exec scraper python scripts/health_check.py
```

**Checks:**
- Disk space availability
- Write permissions
- Network connectivity
- ClickHouse connection

### League Analysis

```bash
# Analyze scraped leagues
python scripts/analyze_leagues.py --days 30

# Show top leagues
python scripts/analyze_leagues.py --top-n 100

# Set minimum match threshold
python scripts/analyze_leagues.py --min-matches 50
```

---

## Performance Metrics

### Bronze Layer

- **Compression**: ~1-2 seconds per 100 matches
- **Space savings**: 60-75% reduction
- **File operations**: Atomic writes with file locking

### Scraping

- **FotMob**: ~0.5-1 second per match
- **AIScore**: ~2-5 seconds per match (with odds)
- **Rate limiting**: Configurable delays

### ClickHouse

- **Batch inserts**: 1000-5000 rows per batch
- **Table optimization**: 5-30 seconds per table
- **Deduplication**: Automatic via ReplacingMergeTree

### Validation

- **Validation overhead**: ~5-10ms per match
- **Response saving**: ~10-20ms per match
- **Total impact**: < 30ms per match (negligible)

---

## Security Considerations

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

---

## Quick Reference

### Environment Variables

```bash
# Essential
FOTMOB_X_MAS_TOKEN=<token>
FOTMOB_API_BASE_URL=https://www.fotmob.com/api/data
CLICKHOUSE_HOST=clickhouse
LOG_LEVEL=INFO

# AIScore
AISCORE_FILTER_BY_LEAGUES=true
AISCORE_ALLOWED_LEAGUES=Premier League,La Liga,...
AISCORE_HEADLESS=true
AISCORE_BROWSER_BLOCK_IMAGES=true
```

### Common Commands

```bash
# Full pipeline
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208

# Bronze only
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208 --bronze-only

# ClickHouse only
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208 --skip-bronze

# Optimize tables
make optimize-tables

# View logs
tail -f logs/pipeline_20251208.log

# Health check
docker-compose -f docker/docker-compose.yml exec scraper python scripts/health_check.py
```

### File Locations

- **Configuration**: `.env`
- **Logs**: `logs/pipeline_*.log`
- **Bronze Data**: `data/{fotmob|aiscore}/matches/YYYYMMDD/`
- **Scripts**: `scripts/*.py`
- **Source Code**: `src/`
- **ClickHouse Schemas**: `clickhouse/init/`

---

**Scout Development Guide** - Technical reference for developers and operators
