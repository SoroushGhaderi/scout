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
14. [Code Review — Current Status & Pitfalls](#code-review--current-status--pitfalls)
    - [What's Working Well](#whats-working-well)
    - [Critical Issues](#critical-issues)
    - [Redundant & Dead Files](#redundant--dead-files)
    - [Architecture Pitfalls](#architecture-pitfalls)
    - [Code Quality Issues](#code-quality-issues)
    - [Recommended Cleanup Roadmap](#recommended-cleanup-roadmap)

---

## Architecture Overview

### System Diagram

```
FotMob REST API          AIScore Web (Selenium)
       │                         │
       ▼                         ▼
 playwright_fetcher         browser.py / browser_pool.py
 daily_scraper.py           scraper.py / odds_scraper.py
 match_scraper.py           extractor.py
       │                         │
       └──────────┬──────────────┘
                  ▼
          Bronze Layer (JSON → GZIP → TAR)
          data/{fotmob|aiscore}/matches/YYYYMMDD/
                  │
                  ▼
        ClickHouse (Analytics DB)
        fotmob.*  (14 tables)
        aiscore.* (5 tables)
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
├── fotmob/
│   ├── matches/YYYYMMDD/
│   │   └── YYYYMMDD_matches.tar        (compressed archive)
│   ├── lineage/YYYYMMDD/
│   │   └── lineage.json
│   └── daily_listings/YYYYMMDD/
│       └── matches.json
└── aiscore/
    ├── matches/YYYYMMDD/
    │   └── YYYYMMDD_matches.tar        (compressed archive)
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
    print(f"Errors: {validation['errors']}")
```

**Standalone Validation:**

```bash
# Validate existing files
python scripts/validate_fotmob_responses.py data/fotmob/matches/20251208

# Generate report
python scripts/validate_fotmob_responses.py data/fotmob/matches \
    --output-dir data/validation_reports
```

---

## Automatic Compression

### Overview

After scraping completes, the system automatically compresses match files into TAR archives, achieving 60-75% space savings.

### Process Flow

```
JSON Files → GZIP (.json.gz) → TAR Archive → Delete .json.gz → Keep .tar
```

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

---

## ClickHouse Optimization

### Overview

Table optimization is separated from data loading for better operational control.

### Usage

```bash
# Optimize all tables (recommended after bulk loading)
make optimize-tables

# Manual optimization
docker-compose -f docker/docker-compose.yml exec clickhouse clickhouse-client \
  --user fotmob_user --password fotmob_pass \
  --query "OPTIMIZE TABLE fotmob.general FINAL DEDUPLICATE"
```

### When to Optimize

- After bulk data loading
- Periodically (daily, weekly, or monthly)
- When query performance degrades
- After large delete/update operations

---

## Logging Improvements

### Log Levels

- **DEBUG**: Element interactions, verification steps
- **INFO**: Milestones, progress (every 10 matches)
- **WARNING**: Recoverable issues, low match counts
- **ERROR**: Critical failures

### Log Output Format

```
[INFO] Starting scraper for 20251209 (headless mode)
[DEBUG] Cloudflare check passed (3s)
[INFO] Located 3 odds tabs: 1X2, Asian Handicap, Over/Under
[INFO] Progress: 10/100 (10%) - Success: 10, Failed: 0
[INFO] Scraping complete: 95/100 successful (95.0%), 4500 odds extracted
```

---

## Extension Points

### Adding New Scrapers

1. Create scraper module in `src/scrapers/{scraper_name}/`
2. Implement base scraper interface from `src/core/interfaces.py`
3. Add configuration in `config/` (not `src/config/`)
4. Update `scripts/pipeline.py`
5. Create ClickHouse schema in `clickhouse/init/`

```
src/scrapers/new_scraper/
├── __init__.py
├── scraper.py          # Main scraper logic
├── bronze_storage.py   # Storage implementation
└── models.py           # Data models
```

### Adding New Data Sources

1. Extend `BaseBronzeStorage` or create scraper-specific storage
2. Add data models in `src/models/`
3. Create processor if transformation needed
4. Update ClickHouse schema

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
docker-compose -f docker/docker-compose.yml exec scraper python scripts/health_check.py
```

**Checks:**
- Disk space availability
- Write permissions
- Network connectivity
- ClickHouse connection

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
- Sensitive data in `.env` file (not tracked in git)
- Application settings in `config.yaml` (tracked in git)
- No hardcoded secrets

---

## Configuration Reference

Scout uses a **two-layer configuration system**:

1. **`config.yaml`** — All application settings (timeouts, delays, selectors, etc.)
2. **`.env`** — Only secrets and environment-specific values

See [CONFIG_GUIDE.md](CONFIG_GUIDE.md) for full documentation.

### Quick Reference

```yaml
# config.yaml
fotmob:
  request:
    timeout: 30
  scraping:
    max_workers: 2

aiscore:
  browser:
    headless: true
```

```bash
# .env
FOTMOB_X_MAS_TOKEN=<token>
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PASSWORD=<password>

# S3 backup (optional)
S3_ENDPOINT=https://...
S3_ACCESS_KEY=<key>
S3_SECRET_KEY=<secret>

# Alerting (optional)
TELEGRAM_BOT_TOKEN=<token>
TELEGRAM_CHAT_ID=<chat_id>
```

---

## Common Commands

```bash
# Full pipeline
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208

# Bronze only (no ClickHouse)
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208 --bronze-only

# ClickHouse only (no scraping)
docker-compose -f docker/docker-compose.yml exec scraper python scripts/pipeline.py 20251208 --skip-bronze

# Optimize tables
make optimize-tables

# View logs
tail -f logs/pipeline_20251208.log

# Health check
docker-compose -f docker/docker-compose.yml exec scraper python scripts/health_check.py
```

### File Locations

| Type | Path |
|---|---|
| Configuration | `config.yaml`, `.env` |
| Logs | `logs/pipeline_*.log` |
| Bronze Data | `data/{fotmob\|aiscore}/matches/YYYYMMDD/` |
| Scripts | `scripts/*.py` |
| Source Code | `src/` |
| ClickHouse Schemas | `clickhouse/init/` |

---

---

# Code Review — Current Status & Pitfalls

> This section is a candid senior-engineer review of the codebase as of Feb 2026.
> It identifies what's solid, what's risky, and what needs to be cleaned up.

---

## What's Working Well

These are genuine strengths worth preserving:

- **Atomic file writes with locking.** The bronze storage layer uses temp-file-then-rename semantics and `fcntl`/`filelock` for concurrent safety. This is correct and production-grade.
- **Bronze → ClickHouse separation.** The two-stage pipeline (scrape to disk first, then load) means a ClickHouse outage never loses raw data. This is the right design.
- **TAR compression pipeline.** JSON → GZIP → TAR achieving 60-75% space savings with transparent decompression on read is well-implemented.
- **ClickHouse table design.** Using `ReplacingMergeTree` on all tables with explicit `ORDER BY` keys gives correct deduplication semantics on re-loads.
- **League filtering.** The allowlist-based filtering for AIScore with 95 competitions prevents bloat from scraping irrelevant matches.
- **Pydantic models for responses.** `src/models/` uses Pydantic v2 for FotMob response validation, which is the right approach.
- **Separation of concerns in scrapers.** FotMob's split into `DailyScraper` + `MatchScraper` + `PlaywrightFetcher` is clean and testable.
- **`BaseBronzeStorage` abstraction.** The common base class for both FotMob and AIScore storage avoids duplication of compression and locking logic.

---

## Critical Issues

These are issues that need to be fixed before treating this codebase as production-stable.

### 1. `fotmob_credentials.py` is a security liability

**File:** `fotmob_credentials.py` (project root)

This file contains real browser session cookies — including Google Analytics IDs, Cloudflare Turnstile tokens, and session identifiers — hardcoded in Python and almost certainly committed to git history.

```python
# fotmob_credentials.py — this is the problem
cookies = {
    '_ga': 'GA1.1.1885161742.1735855707',
    'turnstile_verified': '1.1771434664.35dbc3be...',
    'cto_bundle': 'CZTt_19RZTh2MWdS...',
    # ... more real tokens
}
```

**Why it matters:** Anyone with read access to this repo can impersonate your browser session. The `u:location` cookie also leaks your configured geolocation (`timezone: Asia/Tehran`).

**Fix:** Move credentials to `.env` and read them at runtime:

```bash
# .env
FOTMOB_COOKIE_GA=GA1.1...
FOTMOB_COOKIE_TURNSTILE=1.177...
```

```python
# fotmob_credentials.py — safe version
import os

cookies = {
    '_ga': os.environ['FOTMOB_COOKIE_GA'],
    'turnstile_verified': os.environ['FOTMOB_COOKIE_TURNSTILE'],
}
```

Alternatively, since `scripts/refresh_turnstile.py` already writes a fresh token, that script should write directly to `.env` or a `.credentials.json` that's listed in `.gitignore`.

---

### 2. `src/cli.py` imports from a deprecated module and logs inconsistently

**File:** `src/cli.py`

The CLI still imports from `src.config` (which is explicitly marked deprecated) and uses `from .config import load_config` — triggering a `DeprecationWarning` on every run. It also uses `logger.info("=" * 80)` decorative separators, contradicting the logging standards documented in this file.

```python
# src/cli.py — broken imports
from .config import load_config, FotMobConfig   # ← imports deprecated wrapper
```

**Fix:**
```python
from config import FotMobConfig
```

If `src/cli.py` is no longer the intended entry point (replaced by `scripts/scrape_fotmob.py`), it should be removed outright.

---

### 3. Absolute import in `src/utils/metrics_alerts.py`

**File:** `src/utils/metrics_alerts.py`, line 45

**Status:** ✅ FIXED

```python
from ..storage import get_s3_uploader   # ← now uses relative import
```

Changed from absolute to relative import to ensure compatibility when installed as a package.

---

### 4. `OddsScraper` carries a dead `db` parameter

**File:** `scripts/aiscore_scripts/scrape_odds.py`

```python
def __init__(self, config, db, browser: BrowserManager):
    """Note: db parameter is kept for backward compatibility but ignored."""
```

Dead parameters silently accept wrong arguments without raising errors. Remove `db` from the signature and update all call sites. "Backward compatibility" is not a valid reason to keep a parameter that has no callers outside this repo.

---

## Redundant & Dead Files

The following files serve no active purpose and should be removed. This is the most impactful cleanup you can do for maintainability.

### Remove immediately (deprecated and replaced)

| File | Why |
|---|---|
| `src/config.py` | Explicit `DeprecationWarning` wrapper. All imports already moved to `config/`. Delete it and update `src/cli.py`. |
| `src/config/` (entire directory) | Old config location. `config/` at root is the canonical location. `src/config/` has no importers except `src/config.py` which is itself dead. |
| `src/aggregators/` (entire directory) | Module docstring says "DEPRECATED. Parquet storage has been removed." The `DailyAggregator` class reads from `data/silver/` which no longer exists. Dead code. |
| `src/cli.py` | Replaced by `scripts/scrape_fotmob.py`. Uses deprecated imports. Not referenced by `scripts/pipeline.py`. |
| `src/__main__.py` | Only purpose is to call `src/cli.py`'s `main()`. If `cli.py` is removed, this goes too. |

### Review before removing (possibly still in use)

| File | Issue |
|---|---|
| `scripts/aiscore_scripts/scrape_links.py` | 2,400+ lines in a single script. Functionality is entirely covered by `scripts/scrape_aiscore.py`. If this is kept as a standalone utility, it needs to be split into modules. If not, delete it. |
| `scripts/aiscore_scripts/scrape_odds.py` | Same as above — appears superseded by `scripts/scrape_aiscore.py`. |
| `src/utils/versioning.py` | 548 lines of SCD Type 2 / version history system. There is no evidence this is called anywhere in the active pipeline (`orchestrator.py`, `scripts/pipeline.py`, or either scraper script). If unused, remove it. |
| `src/utils/lineage.py` | Similar to versioning — lineage tracking that may not be wired into the active pipeline. Verify and remove if unused. |
| `src/utils/alerting.py` | General-purpose alert manager (Email + Telegram). `src/utils/metrics_alerts.py` also sends Telegram reports. Two overlapping alerting modules create confusion about which to use. Consolidate or clearly separate responsibilities. |
| `scripts/utils/selenium_utils.py` | Utility functions for Selenium. Verify whether these are used by active scripts or are leftovers from before the refactor. |

### Naming collision worth fixing

`BronzeStorage` is the name of three different classes in three different files:

| Class name | File | Scope |
|---|---|---|
| `BronzeStorage` | `src/storage/bronze_storage.py` | FotMob bronze storage |
| `BronzeStorage` | `src/storage/aiscore_storage.py` | AIScore bronze storage |
| `BronzeStorage` | `src/scrapers/aiscore/bronze_storage.py` | (possible third copy?) |

This is confusing at import time and makes grep results ambiguous. Rename to `FotMobBronzeStorage` and `AIScoreBronzeStorage` (or just `FotMobStorage` / `AIScoreStorage`) and consolidate `src/scrapers/aiscore/bronze_storage.py` into `src/storage/aiscore_storage.py` if they overlap.

---

## Architecture Pitfalls

### 1. Two config package locations

```
config/            ← Canonical (new)
src/config/        ← Old location (should be gone)
```

`src/config/` still exists and contains `fotmob_config.py`, `aiscore_config.py`, and `base.py`. If any script still imports from `src.config.*`, it's using stale code. Run a codebase-wide grep before deleting:

```bash
rg "from src\.config" --type py
rg "from \.config" --type py   # inside src/
```

### 2. `scripts/pipeline.py` vs `src/orchestrator.py` — unclear ownership

`src/orchestrator.py` (`FotMobOrchestrator`) orchestrates FotMob scraping. `scripts/pipeline.py` orchestrates the full two-scraper pipeline. These are at different levels but overlap in responsibility. `pipeline.py` effectively reimplements the date-range loop and retry logic that `orchestrator.py` also handles.

This isn't a bug, but it creates confusion about where to add new pipeline-level logic. Consider: `pipeline.py` should be a thin entry-point that delegates everything to orchestrator classes, not a script with its own business logic.

### 3. `src/utils/versioning.py` — over-engineered and apparently unused

The versioning system (548 lines) implements SCD Type 2, version history, checksums, and reprocessing support. This is a meaningful investment, but only if it's wired into the pipeline. If `bronze_storage.save_match_data()` doesn't call `DataVersioning.save_version()`, then this system exists in isolation and provides no actual guarantees.

**Action:** Either wire it in, or remove it. Unused safety infrastructure is worse than no infrastructure because it creates false confidence.

### 4. `src/utils/health_check.py` vs `scripts/health_check.py`

There is a `HealthChecker` class in `src/utils/health_check.py` and a `scripts/health_check.py` script that calls it. This is fine in principle, but make sure `scripts/health_check.py` doesn't duplicate any logic that should live in the class.

---

## Code Quality Issues

### 1. `scripts/aiscore_scripts/scrape_links.py` is a 2,400-line monolith

A 2,400-line script is unmaintainable. It contains its own `setup_logging()`, its own argument parser, browser management, retry logic, and scraping logic — all in a flat namespace. If this file is kept, it must be split:

```
src/scrapers/aiscore/
├── link_scraper.py      # LinkScraper class — business logic only
└── ...

scripts/
└── scrape_aiscore_links.py   # thin entry point, ~50 lines
```

### 2. `ClientError` imported inside `except` blocks

In `src/storage/s3_uploader.py`, `botocore.exceptions.ClientError` is imported three separate times inside `except` blocks:

```python
except Exception as e:
    try:
        from botocore.exceptions import ClientError   # ← repeated 3x
        if isinstance(e, ClientError) and ...:
            ...
```

Import it once at the module level (it's already gated behind `BOTO3_AVAILABLE`):

```python
if BOTO3_AVAILABLE:
    from botocore.exceptions import ClientError
```

Then use it directly in the `except` clause:

```python
except ClientError as e:
    if e.response['Error']['Code'] in ('404', 'NoSuchKey'):
        return False
```

### 3. Inconsistent logger initialization

The codebase mixes three patterns for logger creation:

```python
# Pattern A — module-level singleton (src/utils/versioning.py)
logger = get_logger()

# Pattern B — instance-level (most scrapers)
self.logger = get_logger()

# Pattern C — stdlib (scripts/aiscore_scripts/)
logger = logging.getLogger(__name__)
```

Pattern C (`logging.getLogger(__name__)`) is the Python standard and the most correct for libraries and modules — it preserves the module hierarchy in log output and allows callers to control log levels per-module. Patterns A and B both return the same singleton regardless of which module calls them, which collapses the entire logging hierarchy into one name.

**Recommendation:** Adopt Pattern C everywhere. In `logging_utils.py`, keep `setup_logging()` for entry-point configuration, but replace `get_logger()` with `logging.getLogger(__name__)` at call sites.

### 4. `src/config/fotmob_config.py` has excessive blank lines

The file opens with 13 blank lines between the docstring and the first import. This is a cosmetic issue but signals the file was heavily edited without cleanup. Run `autopep8` or `ruff format` across the project.

### 5. `ApiConfig.get_headers()` is not deterministic

```python
def get_headers(self, referer: str = "https://www.fotmob.com/") -> Dict[str, str]:
    user_agent = random.choice(self.user_agents)  # ← random on every call
```

Randomly rotating the User-Agent on every request can cause inconsistent fingerprinting mid-session if the same session makes multiple requests. Choose a UA at session start and hold it for the session's lifetime.

### 6. `FotMobConfig` is a dataclass being mutated after construction

In `src/cli.py`:

```python
config = load_config()
if args.no_parallel:
    config.enable_parallel = False   # ← post-construction mutation
```

Dataclasses with mutable fields are fine, but accepting CLI overrides by directly mutating config fields bypasses any validation logic in `__post_init__`. Use a proper override mechanism or `replace()`.

### 7. No `pytest` test suite

There are test-like scripts (`scripts/test_validation.py`) but no `pytest` setup. This means:
- No CI gate on correctness
- No regression protection when refactoring
- The validation scripts need to be run manually

**Minimum viable test setup:**

```bash
pip install pytest pytest-mock
mkdir tests/
# Move test_validation.py logic into tests/test_fotmob_validator.py
pytest tests/
```

---

## Recommended Cleanup Roadmap

Prioritized by impact vs. effort:

### Priority 1 — Security & Correctness (do this week)

- [ ] Move `fotmob_credentials.py` contents to `.env` and `.gitignore` the file, or at minimum add it to `.gitignore` now and purge from git history with `git filter-repo`
- [ ] Fix the absolute import in `src/utils/metrics_alerts.py` (`from src.storage` → `from ..storage`)
- [ ] Remove `db` parameter from `OddsScraper.__init__`

### Priority 2 — Dead Code Removal (do this sprint)

- [ ] Delete `src/config.py`
- [ ] Delete `src/config/` directory (after confirming zero importers via `rg`)
- [ ] Delete `src/aggregators/` directory
- [ ] Delete `src/cli.py` and `src/__main__.py`
- [ ] Audit and remove/consolidate `scripts/aiscore_scripts/` if superseded

### Priority 3 — Architecture (next sprint)

- [ ] Rename `BronzeStorage` classes to avoid the three-way naming collision
- [ ] Consolidate `src/utils/alerting.py` and `src/utils/metrics_alerts.py`
- [ ] Decide on `src/utils/versioning.py` — wire it in or remove it
- [ ] Make `scripts/pipeline.py` a thin entry point; move business logic into `src/`

### Priority 4 — Code Quality (ongoing)

- [ ] Adopt `logging.getLogger(__name__)` uniformly
- [ ] Fix repeated `ClientError` import in `s3_uploader.py`
- [ ] Split `scripts/aiscore_scripts/scrape_links.py` into class + thin script
- [ ] Add `pytest` with at least validation and storage tests
- [ ] Run `ruff format` + `ruff check` and add to CI

---

**Scout Development Guide** — Technical reference for developers and operators
