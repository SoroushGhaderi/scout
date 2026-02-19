# Scout — Full Code Review

> Reviewed: 2026-02-19  
> Reviewer: Senior Engineering Review  
> Codebase: Python 3.11 · FotMob + AIScore scrapers · Bronze → ClickHouse pipeline

---

## 1. Code Analysis

**Scout** is a Python 3.11 data pipeline that scrapes football match data from two sources:

- **FotMob** — REST API with custom x-mas token generation (Playwright + curl_cffi + TLS impersonation)
- **AIScore** — Selenium browser automation with Cloudflare bypass

Data flows: Bronze layer (raw JSON/tar on disk, optional S3) → ClickHouse analytical warehouse. The project uses Pydantic models, pandas DataFrames, a custom exception hierarchy, DLQ, lineage tracking, SCD Type 2 versioning, Telegram/email alerting, and Docker for deployment.

The architecture is well above average for a personal/small-team scraping project. The review below focuses only on real problems, not hypothetical concerns.

---

## 2. Bugs & Issues

### Bug 1 — Logger Is Overwritten in `BronzeStorage.__init__`

**File:** `src/storage/bronze_storage.py` lines 59–68

**Status:** ✅ FIXED (commit 03f863d)

```python
# FIXED — call get_logger AFTER super().__init__
def __init__(self, base_dir: str = "data/fotmob"):
    super().__init__(base_dir)
    self.logger = get_logger()   # override AFTER base sets it
    self.logger.info(f"Bronze storage initialized: {base_dir}")
```

---

### Bug 2 — Race Condition in `mark_match_as_scraped` (No File Lock)

**File:** `src/storage/bronze_storage.py` lines 253–328

```python
# BROKEN — no lock around the read-modify-write
with open(listing_file, 'r', encoding='utf-8') as f:
    data = json.load(f)
# ... modify data ...
temp_file = listing_file.parent / ".matches.json.tmp"
with open(temp_file, 'w', encoding='utf-8') as f:
    json.dump(data, f, ...)
temp_file.rename(listing_file)
```

`save_matches_batch` acquires a `FileLock` before writing, but `mark_match_as_scraped`
does a full read-modify-write cycle with **no lock at all**. Two concurrent workers
marking different matches on the same date will corrupt each other's writes. The atomic
rename protects against a torn write, but not against a lost update race.

**Fix:**

```python
lock_file = listing_file.parent / ".matches.json.lock"
ctx = FileLock(lock_file, timeout=30) if FILE_LOCKING_AVAILABLE else contextlib.nullcontext()
with ctx:
    with open(listing_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    # ... modify ...
    temp_file = listing_file.parent / ".matches.json.tmp"
    with open(temp_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ...)
    temp_file.rename(listing_file)
```

**Why it matters:** Without this, parallel scraper workers silently overwrite each other's
updates. One worker's marked matches will be lost, causing those matches to be re-scraped
on the next run.

---

### Bug 3 — `asyncio.run()` Inside a Synchronous Call Stack

**File:** `src/scrapers/fotmob/playwright_fetcher.py` line 303

```python
result = asyncio.run(_run())
```

`asyncio.run()` raises `RuntimeError: This event loop is already running` when called
from any async context (Jupyter notebooks, async test runners, FastAPI). The current
pipeline is synchronous so this doesn't fail today, but it will silently break if the
caller is ever wrapped in async code. Playwright already ships a synchronous API:

```python
from playwright.sync_api import sync_playwright

def _extract_signing_params_via_playwright(self) -> Dict[str, str]:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()
        page.goto(self.FOTMOB_BASE, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(3_000)
        result = page.evaluate("() => { ... }")
        browser.close()
    if not result or not result.get("foo") or not result.get("h"):
        raise ValueError("Could not find signing params in FotMob webpack")
    return result
```

---

### Bug 4 — SQL Injection Vector via Unvalidated `database` Parameter

**File:** `src/storage/clickhouse_client.py` lines 192–196

```python
# table is validated against an allowlist — good
# db is NOT validated — bad
size_query = (
    f"SELECT formatReadableSize(sum(bytes)) as size, sum(rows) as rows "
    f"FROM system.parts WHERE database = '{db}' AND table = '{table}' AND active"
)
```

`table` is validated via `_validate_table_name`, but `db` (the `database` parameter) is
interpolated directly into the query string. In this project it comes from config, not
user input, but the same pattern is copied into `truncate_table`. Add identifier
validation for database names:

```python
import re
_SAFE_IDENT = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

def _validate_identifier(self, value: str, kind: str = "identifier") -> str:
    if not _SAFE_IDENT.match(value):
        raise ValueError(f"Unsafe {kind}: '{value}'")
    return value

# In get_table_stats and truncate_table:
db = self._validate_identifier(database or self.database, "database")
```

---

### Bug 5 — `compress_date_files` Parses and Re-serializes Every JSON File

**File:** `src/storage/base_bronze_storage.py` lines 836–845

```python
# SLOW — full parse + re-serialize
for json_file in json_files:
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    gz_file = json_file.with_suffix('.json.gz')
    with gzip.open(gz_file, 'wt', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    json_file.unlink()
```

This parses every match JSON to a Python dict and back, throws away the original
whitespace, and wastes CPU. You only need to compress the raw bytes:

```python
# FAST — byte copy, preserves original formatting
import shutil
for json_file in json_files:
    gz_file = json_file.with_suffix('.json.gz')
    with open(json_file, 'rb') as f_in, gzip.open(gz_file, 'wb', compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out)
    json_file.unlink()
```

**Why it matters:** For a day with 300 matches, parsing and re-serializing is 3–5x slower
and risks altering the original data format (e.g. number precision, key ordering).

---

### Bug 6 — Garbled Text in Production Alert Messages

**File:** `src/utils/alerting.py` — 5 locations

**Status:** ✅ FIXED (commit 5431443)

Fixed all 5 garbled text strings in docstrings and messages:
- Line 104: `"Base class for aler in t channels."` → `"Base class for alert channels."`
- Line 416: `"Send alert for dat in a quality issues."` → `"Send alert for data quality issues."`
- Line 429: `f"...detected for matc in h {match_id}..."` → `f"...detected for match {match_id}..."`
- Line 440: `"Send alert for syste in m failure."` → `"Send alert for system failure."`
- Line 463: `"Send alert for healt in h check failure."` → `"Send alert for health check failure."`

---

### Bug 7 — `_collect_all_links` Duplicates 60+ Lines in a "Final Pass"

**File:** `src/scrapers/aiscore/scraper.py` lines 361–406

**Status:** ✅ FIXED (commit e377af2)

Extracted `_process_visible_containers()` helper method that handles:
- Finding visible containers via CSS selector
- Extracting URLs with deduplication
- Building MatchLk objects
- Saving to storage

Both the scroll loop and final pass now call this helper, eliminating ~60 lines of duplicate code.

---

## 3. Best Practices & Standards

### Anti-Pattern: `@dataclass` With Manual `__init__`

**File:** `config/settings.py` lines 47–88

**Status:** ✅ FIXED (commit a2bf740)

Refactored from `@dataclass` with manual `__init__` to use `pydantic_settings.BaseSettings`:

```python
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    environment: Environment = Environment.DEVELOPMENT
    log_level: str = "INFO"
    log_dir: str = "logs"
    data_dir: str = "data"
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "default"
    enable_metrics: bool = True
    enable_health_checks: bool = True

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="allow",
    )
```

This eliminates ~30 lines of manual `os.getenv` boilerplate and adds proper validation with clear error messages.

---

### Anti-Pattern: Abstract Method Without `@abstractmethod`

**File:** `src/utils/alerting.py` lines 130–132

```python
# WRONG — subclasses are optional, not enforced
class AlertChannel:
    def _send_impl(self, alert: Alert) -> bool:
        raise NotImplementedError
```

A concrete `AlertChannel()` can be silently instantiated without implementing `_send_impl`.
Declare the class and method abstract:

```python
from abc import ABC, abstractmethod

class AlertChannel(ABC):
    @abstractmethod
    def _send_impl(self, alert: Alert) -> bool:
        ...
```

---

### Anti-Pattern: Dead Parameters in `BaseScraper.make_request`

**File:** `src/scrapers/fotmob/base_scraper.py` lines 37–58

**Status:** ✅ FIXED (commit c51ccf1)

Removed dead `headers` and `method` parameters that were silently ignored:

```python
def make_request(
    self,
    url: str,
    params: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
```

---

### Anti-Pattern: `pipeline.py` Orchestrates Via `subprocess`

**File:** `scripts/pipeline.py` line 370

```python
return subprocess.run(cmd, cwd=project_root, text=True)
```

The pipeline forks a new Python interpreter to run other scripts **in the same project**.
This means:

- No shared objects or caches (signing params re-extracted every subprocess)
- No exception propagation — only exit codes; stack traces are lost
- New interpreter startup cost per step
- Log files per step that are hard to correlate
- All failures silently absorbed via `continue_on_error=True`

**Fix:** Import and call step functions directly:

```python
from src.scrapers.fotmob.daily_scraper import run_scraping as run_fotmob_scraping
from src.scrapers.aiscore.odds_scraper import run_scraping as run_aiscore_scraping
from src.storage.clickhouse_client import ClickHouseClient

def run_fotmob_bronze(date_str: str, config: PipelineConfig) -> StepResult:
    start = time.time()
    try:
        run_fotmob_scraping(date_str, force=config.force, debug=config.debug)
        return StepResult(name=f"FotMob Bronze - {date_str}", success=True, ...)
    except Exception as exc:
        logger.error(f"FotMob Bronze failed for {date_str}: {exc}", exc_info=True)
        return StepResult(name=f"FotMob Bronze - {date_str}", success=False, ...)
```

Reserve subprocess orchestration for cross-language steps.

---

### Anti-Pattern: Side Effects on Module Import

**File:** `config/settings.py` lines 151–152

```python
settings = Settings()
settings.ensure_directories()   # creates directories on disk at import time
```

Calling `ensure_directories()` at module level means `import config.settings` creates
directories on disk, which:

- Breaks tests using temp directories (they may import this module and pollute the real
  filesystem)
- Makes the module impossible to import in read-only environments
- Creates directories even if the import is just for type-checking or introspection

**Fix:** Move the call to an explicit application startup function:

```python
# config/settings.py
settings = Settings()
# Do NOT call ensure_directories() here

# src/__main__.py  (or wherever the app entry point is)
from config.settings import settings
settings.ensure_directories()
```

---

### Anti-Pattern: Global Mutable Singleton Without Thread Safety

**File:** `src/utils/alerting.py` lines 483–491

```python
_global_alert_manager: Optional[AlertManager] = None

def get_alert_manager() -> AlertManager:
    global _global_alert_manager
    if _global_alert_manager is None:
        _global_alert_manager = AlertManager()   # not thread-safe
    return _global_alert_manager
```

Two threads calling `get_alert_manager()` simultaneously can both see `None` and create
two managers, duplicating Telegram/email channels. It also makes unit tests that call
`get_alert_manager()` share state between test cases.

**Option A — thread-safe singleton:**

```python
import threading
_lock = threading.Lock()

def get_alert_manager() -> AlertManager:
    global _global_alert_manager
    if _global_alert_manager is None:
        with _lock:
            if _global_alert_manager is None:
                _global_alert_manager = AlertManager()
    return _global_alert_manager
```

**Option B (preferred) — dependency injection:**
Pass `AlertManager` as a constructor argument wherever it is needed, eliminating the
global entirely and making all call sites trivially testable.

---

### Minor: Redundant Imports Inside Methods

**File:** `src/scrapers/fotmob/playwright_fetcher.py` lines 422–423

```python
def _read_credentials_file_cookies(self) -> Optional[Dict[str, str]]:
    try:
        import json           # already imported at module top
        from pathlib import Path  # already imported at module top
```

Python caches module imports in `sys.modules`, so this is not a performance issue, but
it signals the method was written incrementally without reviewing existing imports.
Remove both inner imports.

---

### Minor: Anonymous Class Hack

**File:** `scripts/pipeline.py` line 396

```python
return type('Result', (), {'returncode': process.returncode})()
```

Replace with the standard library equivalent:

```python
from types import SimpleNamespace
return SimpleNamespace(returncode=process.returncode)
```

Or better, use a proper `subprocess.CompletedProcess`-compatible dataclass.

---

## 4. What's Done Well

**The x-mas token implementation is genuinely impressive.**
Reverse-engineering FotMob's webpack-bundled signing algorithm, implementing it in pure
Python (MD5 + base64), and adding a Playwright-based live-extraction with a 24-hour TTL
refresh and a hardcoded fallback is both clever and operationally robust. The three-tier
fallback chain (live extraction → cached in-memory → hardcoded constant) is exactly right.

**Atomic write pattern is applied consistently.**
Every JSON write goes through `temp_path` → verify JSON valid → rename. This prevents
corrupt half-written files from surviving a crash.

**The exception hierarchy is clean and used correctly.**
`StorageError → StorageWriteError / StorageReadError` and
`ScraperError → NetworkError / CloudflareError / ElementNotFoundError / BrowserError`
are well-structured and the `tenacity` retry decorators correctly limit retries to only
the retriable exceptions (`NetworkError`, `BrowserError`), not logic errors.

**`ClickHouseClient` table name allowlist is the right security call.**
It may look over-engineered but is exactly what you want before anything that could
eventually be caller-influenced touches SQL.

**Cloudflare cookie management strategy is well-designed.**
The priority chain (Chrome `browser-cookie3` → `credentials.json` fallback) with
JWT-timestamp expiry validation for `turnstile_verified` is the correct operational
approach. The `_is_turnstile_valid` method handles unknown formats optimistically, which
is the right default.

**The compression pipeline is correct and resumable.**
The three-step JSON → gzip → tar flow, with archive integrity verification (members set
comparison) and cleanup on failure, is solid. The `force=False` resume check is a good
operational detail that prevents re-compressing already-archived data.

**`PipelineResults.get_summary()` uses `getattr(self, category)` cleanly**
to iterate dynamically over the four named result lists without a dict or match
statement. Clean use of Python's attribute model.

---

## 5. Architecture & Scalability

### `mark_match_as_scraped` Is O(n²)

For a day with N matches, `mark_match_as_scraped` does:
- 1 `json.load` of the full listing
- Re-computes storage stats (scans the whole matches directory)
- 1 `json.dump` of the full listing

...for each of the N matches scraped. That's N full file reads + N directory scans +
N full file writes as the listing grows. For 300 matches, this is 300× read-modify-write
of the same file.

**Fix:** Accumulate marks in memory during the scraping session and flush once at the end:

```python
class BronzeStorage(BaseBronzeStorage):
    def __init__(self, ...):
        super().__init__(...)
        self._pending_scraped: Dict[str, set] = {}  # date -> set of match IDs

    def mark_match_as_scraped(self, match_id: str, date_str: str) -> None:
        date_normalized = self._normalize_date(date_str)
        self._pending_scraped.setdefault(date_normalized, set()).add(match_id)

    def flush_scraped_marks(self) -> None:
        """Write all pending marks to disk. Call once after a scraping session."""
        for date_str, match_ids in self._pending_scraped.items():
            self._write_scraped_marks(date_str, match_ids)
        self._pending_scraped.clear()
```

### Deprecated Files Should Be Removed

**Status:** ✅ PARTIALLY FIXED (commit 8b2b0b6)

Some files marked deprecated have been removed:

| File | Status |
|------|--------|
| `src/config.py` | ✅ Deleted (commit 8b2b0b6) |
| `src/__main__.py` | ✅ Deleted (commit 8b2b0b6) |
| `src/scrapers/aiscore/config.py` | Still in use — DO NOT remove |
| `src/scrapers/aiscore/bronze_storage.py` | ✅ Moved to `src/storage/aiscore_storage.py` |

The deprecated `src/scrapers/aiscore/config.py` uses relative imports and is still actively imported by AIScore scraper modules.

### `load_raw_match_data` Without `date_str` Is Expensive

```python
# src/storage/base_bronze_storage.py lines 462–463
matches_gz = list(self.matches_dir.rglob(f"match_{match_id}.json.gz"))
matches    = list(self.matches_dir.rglob(f"match_{match_id}.json"))
```

A recursive glob over 30 date subdirectories × 300 files each is a linear scan of ~9,000
inodes. In practice, `date_str` is always known at call sites. Make it required and
remove the no-date code path, or build a match-ID-to-date in-memory index on first use.

### Configuration Duplication: `config/` vs `src/scrapers/aiscore/config.py`

There is a `config/` package at the project root (canonical) and a `config.py` inside the
AIScore scraper (deprecated). Any new code should import only from `config/`. Delete the
deprecated file once confirmed unused.

---

## 6. Testability

The codebase has `pytest.ini` configured but effectively zero unit tests for core logic.

### What Should Be Tested First

**`PlaywrightFetcher._generate_xmas` — pure function, zero dependencies:**

```python
def test_xmas_token_structure():
    fetcher = PlaywrightFetcher(config=mock_config)
    fetcher._foo_hash = "b7d7a67fdaf7133d2d86e74f10192829827d674c"
    fetcher._h_lyrics = "test_lyrics"
    token = fetcher._generate_xmas("/api/data/matches?date=20250101")
    decoded = json.loads(base64.b64decode(token))
    assert set(decoded.keys()) == {"body", "signature"}
    assert decoded["body"]["url"] == "/api/data/matches?date=20250101"
    assert len(decoded["signature"]) == 32   # MD5 hex upper
    assert decoded["signature"] == decoded["signature"].upper()
```

**`BaseBronzeStorage` — already supports `tmp_path` injection:**

```python
def test_save_and_load_round_trip(tmp_path):
    storage = BronzeStorage(base_dir=str(tmp_path / "fotmob"))
    storage.save_raw_match_data("12345", {"goals": 2}, date_str="20250101")
    data = storage.load_raw_match_data("12345", date_str="20250101")
    assert data == {"goals": 2}

def test_atomic_write_leaves_no_temp_file_on_success(tmp_path):
    storage = BronzeStorage(base_dir=str(tmp_path / "fotmob"))
    storage.save_raw_match_data("99999", {}, date_str="20250101")
    tmp_files = list((tmp_path / "fotmob").rglob("*.tmp"))
    assert tmp_files == []
```

**`AlertManager` — fix global state for test isolation:**

```python
# conftest.py
import src.utils.alerting as alerting_module

@pytest.fixture(autouse=True)
def reset_alert_manager():
    yield
    alerting_module._global_alert_manager = None
```

### Making `FootballScraper` Testable

`FootballScraper` is tightly coupled to a live Selenium `WebDriver`. Introduce an
interface:

```python
from abc import ABC, abstractmethod

class AbstractBrowser(ABC):
    @abstractmethod
    def get(self, url: str) -> None: ...
    @abstractmethod
    def find_elements(self, by: str, value: str) -> list: ...
    @abstractmethod
    def scroll_page(self, pixels: int) -> None: ...
    @abstractmethod
    def is_at_bottom(self) -> bool: ...
```

Inject a `MockBrowser` in tests, removing the Selenium dependency from unit test runs.

---

## 7. Overall Scores

| Dimension | Score | Notes |
|---|---|---|
| **Readability** | 8/10 | Good naming, docstrings, and type hints throughout. Some methods exceed 200 lines. |
| **Maintainability** | 7/10 | Improved: dead files removed, duplicate code eliminated. Still: subprocess pipeline and some global state. |
| **Performance** | 6/10 | JSON parse-on-compress, 300× read-modify-write for match marking, and `rglob` without a date are the main bottlenecks. |
| **Security** | 7/10 | Table allowlist is correct. `db` parameter not validated in SQL. Hardcoded signing constants are an acceptable tradeoff given the fallback architecture. |
| **Testability** | 4/10 | Side effects on import, global singletons, tight Selenium coupling, and near-zero test coverage are the main gaps. |

---

## 8. Prioritized Action List

### Fix NOW — bugs causing incorrect behavior in production

1. ✅ ~~**Add `FileLock` to `mark_match_as_scraped`**~~ — race condition causes silent data loss (NOT YET FIXED)
2. ✅ **Fix 5 garbled strings in `alerting.py`** — FIXED (commit 5431443)
3. ✅ **Fix logger overwrite in `BronzeStorage.__init__`** — FIXED (commit 03f863d)
4. **Replace `asyncio.run()` with sync Playwright API** — will silently break in any async context (NOT YET FIXED)
5. **Fix `compress_date_files` to copy bytes, not parse+reserialize** — data format alteration risk and unnecessary CPU cost (NOT YET FIXED)

### Fix SOON — design problems that add maintenance cost

6. ✅ **Remove deprecated files** — PARTIALLY FIXED: `src/config.py` and `src/__main__.py` deleted (commit 8b2b0b6)
7. **Add `database` identifier validation in `ClickHouseClient`** (NOT YET FIXED)
8. ✅ **Replace `@dataclass` + manual `__init__` in `Settings`** — FIXED (commit a2bf740)
9. ✅ **Extract `_process_visible_containers()` helper** — FIXED (commit e377af2)
10. **Move `settings.ensure_directories()` out of module-level import** (NOT YET FIXED)
11. **Declare `AlertChannel` as `ABC`** and `_send_impl` as `@abstractmethod` (NOT YET FIXED)
12. ✅ **Remove dead `headers` and `method` parameters from `BaseScraper.make_request`** — FIXED (commit c51ccf1)

### Improve LATER — architecture and scale

13. **Refactor `pipeline.py` from subprocess to direct function calls** — shared caches, proper exception propagation, unified logging
14. **Batch `mark_match_as_scraped` updates** — accumulate in memory, flush once per session, eliminating the O(n²) read-modify-write pattern
15. **Add a test suite** — start with `_generate_xmas` (pure), `BaseBronzeStorage` (injectable `tmp_path`), and `AlertManager` (mock channels)
16. **Make `AlertManager` injectable** — remove global singleton for testability
17. **Add `AbstractBrowser` interface to `FootballScraper`** — decouple from Selenium for unit testing
18. **Make `date_str` required in `load_raw_match_data`** and remove the expensive `rglob` no-date code path
