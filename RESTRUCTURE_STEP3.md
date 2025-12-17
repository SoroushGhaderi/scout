# Project Restructuring - Step 3: Create Core Interfaces

**Date:** December 18, 2025  
**Status:** ✅ COMPLETED

## Summary

Created `src/core/` package with interfaces, exceptions, constants, and type definitions. This establishes formal contracts for all components and provides a solid foundation for the entire application architecture.

## Changes Made

### 1. Created Core Package Structure
```
src/core/
├── __init__.py         # Package exports
├── interfaces.py       # Protocol definitions
├── exceptions.py       # Custom exception hierarchy
├── constants.py        # Project-wide constants
├── types.py            # Type aliases and validators
└── README.md           # Comprehensive documentation
```

### 2. Created `interfaces.py` - Protocol Definitions

**8 Protocols Defined:**
1. `StorageProtocol` - Storage implementations interface
2. `ScraperProtocol` - Scraper implementations interface
3. `ProcessorProtocol` - Data processor interface
4. `OrchestratorProtocol` - Orchestration logic interface
5. `ConfigProtocol` - Configuration interface
6. `CacheProtocol` - Cache implementations interface
7. `MetricsProtocol` - Metrics tracking interface
8. `LoggerProtocol` - Logging interface

**Usage Example:**
```python
from src.core import StorageProtocol, ScraperProtocol

def process_data(storage: StorageProtocol, scraper: ScraperProtocol):
    data = scraper.fetch_match_details("12345")
    storage.save_match("12345", data, "20241218")
```

### 3. Created `exceptions.py` - Exception Hierarchy

**Complete Exception Tree:**
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
│   ├── ScraperParseError
│   ├── ScraperAuthenticationError
│   └── ScraperCloudflareChallengeError
├── ProcessorError
│   └── ValidationError
├── DatabaseError
│   ├── DatabaseConnectionError
│   └── DatabaseQueryError
└── OrchestratorError
```

**Features:**
- Structured error information with `details` dict
- `to_dict()` method for serialization
- Rich context in error messages
- Easy error categorization

**Usage Example:**
```python
from src.core import StorageError, StorageWriteError

try:
    storage.save_match(match_id, data, date)
except StorageWriteError as e:
    logger.error(f"Failed to save: {e}")
    logger.debug(f"Details: {e.to_dict()}")
```

### 4. Created `constants.py` - Project Constants

**10 Constant Categories:**
1. **Date/Time Formats** - `DATE_FORMAT_COMPACT`, `DATE_FORMAT_DISPLAY`, etc.
2. **HTTP Status** - `HttpStatus.OK`, `HttpStatus.RATE_LIMITED`, etc.
3. **File Extensions** - `FileExtension.JSON`, `FileExtension.TAR`, etc.
4. **Match Statuses** - `MatchStatus.FINISHED`, `MatchStatus.COMPLETED_STATUSES`, etc.
5. **Scraper Names** - `Scraper.FOTMOB`, `Scraper.AISCORE`
6. **Storage Layers** - `StorageLayer.BRONZE`, `SILVER`, `GOLD`
7. **Defaults** - Timeouts, retries, batch sizes, cache TTL
8. **Environment Variables** - `EnvVar.FOTMOB_X_MAS_TOKEN`, etc.
9. **Regex Patterns** - `Pattern.DATE_YYYYMMDD`, etc.
10. **Table Names** - ClickHouse table identifiers
11. **Error Messages** - Common error message templates

**Usage Example:**
```python
from src.core.constants import MatchStatus, Defaults, HttpStatus

if response.status_code == HttpStatus.RATE_LIMITED:
    time.sleep(Defaults.RETRY_INITIAL_WAIT)

if match_status in MatchStatus.COMPLETED_STATUSES:
    process_match(match)
```

### 5. Created `types.py` - Type Definitions

**Type Categories:**
- **Basic Aliases:** `MatchID`, `DateStr`, `URL`, `JSONDict`
- **Status Types:** `MatchStatusType`, `ScrapeStatus`, `Environment`
- **Structured Types:** `TeamData`, `MatchMetadata`, `ScraperMetrics`, etc.
- **Function Returns:** `MatchDataResult`, `ValidationResult`, etc.
- **Validators:** `is_valid_match_id()`, `is_valid_date_str()`, etc.

**Usage Example:**
```python
from src.core.types import MatchID, DateStr, JSONDict, is_valid_date_str

def scrape_match(match_id: MatchID, date: DateStr) -> JSONDict:
    if not is_valid_date_str(date):
        raise ValueError(f"Invalid date: {date}")
    ...
```

### 6. Created Comprehensive README

- Overview of all modules
- Usage examples for each component
- Integration examples
- Testing patterns with protocols
- Best practices
- Migration guide
- Contributing guidelines

## Architecture Improvements

### Before (No Core)
```python
# Scattered magic values
if response.status_code == 429:  # What does 429 mean?
    time.sleep(2)  # Why 2 seconds?
    raise Exception("Rate limited")  # Generic exception

# No type hints
def save_match(match_id, data, date):
    pass

# Direct coupling
class MyOrchestrator:
    def __init__(self):
        self.storage = BronzeStorage()  # Hard-coded dependency
```

### After (With Core) ✅
```python
from src.core import StorageProtocol, ScraperRateLimitError
from src.core.constants import HttpStatus, Defaults
from src.core.types import MatchID, DateStr, JSONDict

# Named constants
if response.status_code == HttpStatus.RATE_LIMITED:
    time.sleep(Defaults.RETRY_INITIAL_WAIT)
    raise ScraperRateLimitError("Rate limited", details={'url': url})

# Type-safe signatures
def save_match(match_id: MatchID, data: JSONDict, date: DateStr) -> Path:
    pass

# Protocol-based dependency injection
class MyOrchestrator:
    def __init__(self, storage: StorageProtocol):
        self.storage = storage  # Any storage implementation
```

## Why This Matters

1. **Type Safety** - Catch errors at development time with mypy
2. **Testability** - Easy to mock dependencies using protocols
3. **Maintainability** - Clear contracts between components
4. **Consistency** - Standard error handling across codebase
5. **Readability** - Named constants instead of magic values
6. **Documentation** - Protocols serve as living documentation
7. **Flexibility** - Easy to swap implementations

## Benefits

✅ **Protocol-based architecture** - Structural typing for flexibility  
✅ **Comprehensive exception hierarchy** - Proper error categorization  
✅ **Centralized constants** - No more magic numbers/strings  
✅ **Type safety** - Better IDE support and type checking  
✅ **Testability** - Easy mocking with protocol types  
✅ **Documentation** - Self-documenting code through types  
✅ **Consistency** - Standard patterns across all modules  

## Integration Examples

### Example 1: Mock Storage for Testing
```python
from src.core import StorageProtocol
from src.core.types import MatchID, DateStr, JSONDict

class MockStorage(StorageProtocol):
    def __init__(self):
        self.data = {}
    
    def save_match(self, match_id: MatchID, data: JSONDict, date: DateStr):
        self.data[match_id] = data
        return Path(f"/mock/{match_id}.json")
    
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

### Example 2: Type-Safe Error Handling
```python
from src.core import ScraperError, StorageError, ValidationError
from src.core.exceptions import format_error

try:
    data = scraper.fetch_match("12345")
    storage.save_match("12345", data, "20241218")
except ScraperTimeoutError as e:
    logger.warning(f"Scraper timeout: {e}")
    # Retry logic
except StorageWriteError as e:
    logger.error(f"Storage failed: {e.to_dict()}")
    # Alert on-call
except ValidationError as e:
    logger.error(f"Invalid data: field={e.field}, value={e.value}")
    # Skip this match
```

### Example 3: Using Constants
```python
from src.core.constants import (
    MatchStatus,
    Defaults,
    HttpStatus,
    DATE_FORMAT_COMPACT,
)

# Filter completed matches
completed = [
    match for match in matches
    if match['status'] in MatchStatus.COMPLETED_STATUSES
]

# Use default timeout
response = requests.get(url, timeout=Defaults.HTTP_TIMEOUT)

# Handle rate limiting
if response.status_code == HttpStatus.RATE_LIMITED:
    time.sleep(Defaults.RETRY_INITIAL_WAIT)
```

## Files Created

```
src/core/
├── __init__.py         (90 lines)  - Package exports
├── interfaces.py       (380 lines) - Protocol definitions
├── exceptions.py       (350 lines) - Exception hierarchy
├── constants.py        (270 lines) - Project constants
├── types.py            (280 lines) - Type definitions
└── README.md           (450 lines) - Documentation
```

**Total:** ~1,820 lines of foundational code

## Testing

The core package is designed to be easily testable:

```python
# Test with protocols
from src.core import StorageProtocol

def test_storage(storage: StorageProtocol):
    """Test any storage implementation."""
    storage.save_match("123", {"score": "2-1"}, "20241218")
    assert storage.match_exists("123", "20241218")
    data = storage.load_match("123", "20241218")
    assert data["score"] == "2-1"

# Run with any storage
test_storage(BronzeStorage())
test_storage(MockStorage())
test_storage(RedisStorage())
```

## Next Steps (Future)

**Step 4:** Move Docker files to `docker/` directory  
**Step 5:** Move notebooks to `notebooks/` directory  
**Step 6:** Update existing code to use core interfaces  
**Step 7:** Add mypy type checking to CI/CD  

---

## Commit Message (Suggested)

```
feat(core): Add core interfaces, exceptions, constants, and types

Create src/core/ package with foundational components:

- interfaces.py: 8 protocols for all major components
  * StorageProtocol, ScraperProtocol, ProcessorProtocol
  * OrchestratorProtocol, ConfigProtocol, CacheProtocol
  * MetricsProtocol, LoggerProtocol
  
- exceptions.py: Complete exception hierarchy
  * 15+ custom exceptions inheriting from ScoutError
  * Structured error information with details dict
  * to_dict() method for serialization
  
- constants.py: Project-wide constants
  * Date formats, HTTP status codes, match statuses
  * Default values, environment variables, patterns
  * Table names, error message templates
  
- types.py: Type aliases and validators
  * MatchID, DateStr, JSONDict, and more
  * Structured types with TypedDict
  * Type validators (is_valid_date_str, etc.)
  
- README.md: Comprehensive documentation
  * Usage examples for all modules
  * Integration patterns and best practices
  * Migration guide from old patterns

Benefits:
- Protocol-based architecture for flexibility
- Type safety with mypy support
- Proper error categorization
- No more magic numbers/strings
- Easy testing with mock implementations
- Self-documenting code through types

This establishes formal contracts between components and provides
a solid foundation for the entire application.
```

---

## Impact

### Code Quality
- ✅ Type hints throughout
- ✅ Protocol-based design
- ✅ Proper error handling
- ✅ Named constants

### Developer Experience
- ✅ Better IDE autocomplete
- ✅ Clear API contracts
- ✅ Comprehensive documentation
- ✅ Easy to understand patterns

### Maintainability
- ✅ Single source of truth for constants
- ✅ Easy to add new implementations
- ✅ Clear dependency contracts
- ✅ Testable components

### Future-Proofing
- ✅ Ready for dependency injection
- ✅ Easy to swap implementations
- ✅ Supports microservices architecture
- ✅ Plugin-friendly design
